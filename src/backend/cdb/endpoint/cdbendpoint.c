/*-------------------------------------------------------------------------
 * cdbendpoint.c
 *
 * An endpoint is a query result source for a parallel retrieve cursor on a
 * dedicated QE. One parallel retrieve cursor could have multiple endpoints
 * on different QEs to allow the retrieving to be done in parallel.
 *
 * This file implements the sender part of endpoint.
 *
 * Endpoint may exist on master or segments, depends on the query of the PARALLEL
 * RETRIEVE CURSOR:
 * (1) An endpoint is on QD only if the query of the parallel cursor needs to be
 *	   finally gathered by the master. e.g.:
 * > DECLCARE c1 PARALLEL RETRIEVE CURSOR FOR SELECT * FROM T1 ORDER BY C1;
 * (2) The endpoints are on specific segments node if the direct dispatch happens.
 *	   e.g.:
 * > DECLCARE c1 PARALLEL RETRIEVE CURSOR FOR SELECT * FROM T1 WHERE C1=1 OR C1=2;
 * (3) The endpoints are on all segments node. e.g:
 * > DECLCARE c1 PARALLEL RETRIEVE CURSOR FOR SELECT * FROM T1;
 *
 * When a parallel retrieve cusor is declared, the query plan will be dispatched
 * to the corresponding QEs. Before the query execution, endpoints will be
 * created first on QEs. An entry of EndpointDesc in the shared memory represents
 * the endpoint. Through the EndpointDesc, the client could know the endpoint's
 * identification (endpoint name), location (dbid, host, port and session id),
 * and the status for the retrieve session. All of those information can be
 * obtained on QD by UDF "gp_endpoints_info" or on QE's retrieve session by UDF
 * "gp_endpoint_status_info". The EndpointDesc are stored on QE only in the
 * shared memory. QD doesn't know the endpoint's information unless it sends a
 * query requst (by UDF "gp_endpoint_status_info") to QE.
 *
 * Instead of returning the query result to master through a normal dest receiver,
 * endpoints writes the results to TQueueDestReceiver which is a shared memory
 * queue and can be retrieved from a different process. See
 * CreateTQDestReceiverForEndpoint(). The information about the message queue is
 * also stored in the EndpointDesc so that the retrieve session on the same QE
 * can know.
 *
 * The token is stored in a different structure SessionInfoEntry to make the
 * tokens same for all endpoints in the same session. The token is created on
 * each QE after plan get dispatched.
 *
 * DECLCARE returns only when endpoint and token are ready and query starts
 * execution. See WaitEndpointReady().
 *
 * When the query finishes, the endpoint won't be destroyed immediately since we
 * may still want to check its status on QD. In the implementation, the
 * DestroyTQDestReceiverForEndpoint is blocked until the parallel retrieve cursor
 * is closed explicitly through CLOSE statement or error happens.
 *
 * About implementation of endpoint receiver, see "cdbendpointretrieve.c".
 *
 * UDF gp_check_parallel_retrieve_cursor and gp_wait_parallel_retrieve_cursor are
 * supplied as client helper functions to monitor the retrieve status through
 * QD - QE libpq connection.
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *		src/backend/cdb/cdbendpoint.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tupdesc.h"
#include "access/xact.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbendpoint.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdbendpointinternal.h"
#include "libpq-fe.h"
#include "libpq/libpq.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "utils/backend_cancel.h"
#include "utils/builtins.h"
#include "utils/portal.h"
#include "utils/elog.h"
#ifdef FAULT_INJECTOR
#include "utils/faultinjector.h"
#endif

/* The timeout before returns failure for endpoints initialization. */
#define WAIT_NORMAL_TIMEOUT				100
/* This value is copy from PG's PARALLEL_TUPLE_QUEUE_SIZE */
#define ENDPOINT_TUPLE_QUEUE_SIZE		65536

#define SHMEM_ENDPOINTS_ENTRIES			"SharedMemoryEndpointDescEntries"
#define SHMEM_ENPOINTS_SESSION_INFO		"EndpointsSessionInfosHashtable"

#ifdef FAULT_INJECTOR
#define DUMMY_ENDPOINT_NAME "DUMMYENDPOINTNAME"
#define DUMMY_CURSOR_NAME	"DUMMYCURSORNAME"
#endif

typedef struct SessionTokenTag
{
	int			sessionID;
	Oid			userID;
}	SessionTokenTag;

/*
 * sharedSessionInfoHash is located in shared memory on each segment for
 * authentication purpose.
 *
 * For each session, generate auth token and create SessionInfoEntry for
 * each user who 'DECLARE PARALLEL CURSOR'.
 * Once session exit, clean entries for current session.
 *
 * The issue here is that there is no way to register clean function during
 * session exit on segments(QE exit does not mean session exit). So we
 * register transaction callback(session_info_clean_callback) to clean
 * entries for each transaction exit callback instead. And create new entry
 * if not exists.
 *
 * Since in a transaction, user can 'SET ROLE' to a different user, sessionUserList
 * is used to track userIDs. When clean callback(session_info_clean_callback)
 * executes, it removes all entries for these users.
 */
typedef struct SessionInfoEntry
{
	SessionTokenTag tag;

	/* The auth token for this session. */
	int8		token[ENDPOINT_TOKEN_LEN];
	/* How many endpoint are refered to this entry. */
	uint16      endpointCounter;
}	SessionInfoEntry;

extern Datum gp_check_parallel_retrieve_cursor(PG_FUNCTION_ARGS);
extern Datum gp_wait_parallel_retrieve_cursor(PG_FUNCTION_ARGS);

/* Shared hash table for session infos */
static HTAB *sharedSessionInfoHash = NULL;
/* Track userIDs to clean up SessionInfoEntry */
static List *sessionUserList = NULL;

/* Point to EndpointDesc entries in shared memory */
static EndpointDesc *sharedEndpoints = NULL;

/* Current EndpointDesc entry for sender.
 * It is set when create dest receiver and unset when destroy it. */
static volatile EndpointDesc *activeSharedEndpoint = NULL;

/* Current dsm_segment pointer, saved it for detach. */
static dsm_segment *activeDsmSeg = NULL;

/* Init helper functions */
static void init_shared_endpoints(void *address);

/* Token utility functions */
static const int8 *get_or_create_token(void);

/* Endpoint helper function */
static EndpointDesc *alloc_endpoint(const char *cursorName, dsm_handle dsmHandle);
static void free_endpoint(volatile EndpointDesc * endpoint);
static void create_and_connect_mq(TupleDesc tupleDesc,
					  dsm_segment **mqSeg /* out */ ,
					  shm_mq_handle **mqHandle /* out */ );
static void detach_mq(dsm_segment *dsmSeg);
static void init_session_info_entry(void);
static void wait_receiver(void);
static void unset_endpoint_sender_pid(volatile EndpointDesc * endPointDesc);
static void signal_receiver_abort(pid_t receiverPid,
					  enum AttachStatus attachStatus);
static void endpoint_abort(void);
static void wait_parallel_retrieve_close(void);
static void register_endpoint_callbacks(void);
static void sender_xact_abort_callback(XactEvent ev, void *vp);
static void sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						SubTransactionId parentSubid, void *arg);

/* utility */
static void generate_endpoint_name(char *name, const char *cursorName,
					   int32 sessionID, int32 segindex);
static void check_dispatch_connection(void);

/* Endpoints internal operation UDF's helper function */
static void session_info_clean_callback(XactEvent ev, void *vp);
static bool check_parallel_retrieve_cursor(const char *cursorName, bool isWait);
static void check_parallel_cursor_errors(EState *estate);

/*
 * Endpoint_ShmemSize - Calculate the shared memory size for PARALLEL RETRIEVE
 * CURSOR execute.
 *
 * The size contains LWLocks and EndpointSharedCTX.
 */
Size
EndpointShmemSize(void)
{
	Size		size;

	size = MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc)));
	size = add_size(
	  size, hash_estimate_size(MAX_ENDPOINT_SIZE, sizeof(SessionInfoEntry)));
	return size;
}

/*
 * Endpoint_CTX_ShmemInit - Init shared memory structure for PARALLEL RETRIEVE
 * CURSOR execute.
 */
void
EndpointCTXShmemInit(void)
{
	bool		isShmemReady;
	HASHCTL		hctl;

	sharedEndpoints = (EndpointDesc *) ShmemInitStruct(
													 SHMEM_ENDPOINTS_ENTRIES,
				 MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc))),
													   &isShmemReady);
	Assert(isShmemReady || !IsUnderPostmaster);
	if (!isShmemReady)
	{
		init_shared_endpoints(sharedEndpoints);
	}

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(SessionTokenTag);
	hctl.entrysize = sizeof(SessionInfoEntry);
	hctl.hash = tag_hash;
	sharedSessionInfoHash =
		ShmemInitHash(SHMEM_ENPOINTS_SESSION_INFO, MAX_ENDPOINT_SIZE,
					  MAX_ENDPOINT_SIZE, &hctl, HASH_ELEM | HASH_FUNCTION);
}

/*
 * Init EndpointDesc entries.
 */
static void
init_shared_endpoints(void *address)
{
	Endpoint	endpoints = (Endpoint) address;

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		endpoints[i].databaseID = InvalidOid;
		endpoints[i].senderPid = InvalidPid;
		endpoints[i].receiverPid = InvalidPid;
		endpoints[i].mqDsmHandle = DSM_HANDLE_INVALID;
		endpoints[i].sessionID = InvalidSession;
		endpoints[i].userID = InvalidOid;
		endpoints[i].attachStatus = Status_Invalid;
		endpoints[i].empty = true;
		InitSharedLatch(&endpoints[i].ackDone);
	}
}

/*
 * GetParallelCursorEndpointPosition - get PARALLEL RETRIEVE CURSOR endpoint
 * allocate position
 *
 * If already focused and flow is CdbLocusType_SingleQE, CdbLocusType_Entry,
 * we assume the endpoint should be existed on QD. Else, on QEs.
 */
enum EndPointExecPosition
GetParallelCursorEndpointPosition(const struct Plan *planTree)
{
	if (planTree->flow->flotype == FLOW_SINGLETON &&
		planTree->flow->locustype != CdbLocusType_SegmentGeneral)
	{
		return ENDPOINT_ON_ENTRY_DB;
	}
	else
	{
		if (planTree->flow->flotype == FLOW_SINGLETON)
		{
			/*
			 * In this case, the plan is for replicated table. locustype must
			 * be CdbLocusType_SegmentGeneral.
			 */
			Assert(planTree->flow->locustype == CdbLocusType_SegmentGeneral);
			return ENDPOINT_ON_SINGLE_QE;
		}
		else if (planTree->directDispatch.isDirectDispatch &&
				 planTree->directDispatch.contentIds != NULL)
		{
			/*
			 * Direct dispatch to some segments, so end-points only exist on
			 * these segments
			 */
			return ENDPOINT_ON_SOME_QE;
		}
		else
		{
			return ENDPOINT_ON_ALL_QE;
		}
	}
}

/*
 * ChooseEndpointContentIDForParallelCursor - choose endpoints position base on
 * plan.
 *
 * Base on different condition, we need figure out which
 * segments that endpoints should be allocated in.
 */
List *
ChooseEndpointContentIDForParallelCursor(const struct Plan *planTree,
										 enum EndPointExecPosition * position)
{
	List	   *cids = NIL;

	*position = GetParallelCursorEndpointPosition(planTree);
	switch (*position)
	{
		case ENDPOINT_ON_ENTRY_DB:
			{
				cids = list_make1_int(MASTER_CONTENT_ID);
				break;
			}
		case ENDPOINT_ON_SINGLE_QE:
			{
				cids = list_make1_int(gp_session_id % planTree->flow->numsegments);
				break;
			}
		case ENDPOINT_ON_SOME_QE:
			{
				ListCell   *cell;

				foreach(cell, planTree->directDispatch.contentIds)
				{
					int			contentid = lfirst_int(cell);

					cids = lappend_int(cids, contentid);
				}
				break;
			}
		case ENDPOINT_ON_ALL_QE:
		default:
			break;
	}
	return cids;
}

/*
 * WaitEndpointReady - wait until the PARALLEL RETRIEVE CURSOR ready for retrieve
 *
 * On QD, after dispatch the plan to QEs, QD will wait for QEs' ENDPOINT_READY
 * acknowledge NOTIFY message. Then, we know all endpoints are ready for retrieve.
 */
void
WaitEndpointReady(EState *estate)
{
	Assert(estate);
	CdbDispatcherState* ds = estate->dispatcherState;

	cdbdisp_waitDispatchAckMessage(ds, ENDPOINT_READY_ACK, true);
	check_parallel_cursor_errors(estate);
}

/*
 * Get or create a authentication token for current session.
 * Token is unique for every session id. This is guaranteed by using the session
 * id as a part of the token. And same session will have the same token. Thus the
 * retriever will know which session to attach when doing authentication.
 */
const int8 *
get_or_create_token(void)
{
#ifdef HAVE_STRONG_RANDOM
	static int	sessionId = InvalidSession;
	static int8 currentToken[ENDPOINT_TOKEN_LEN] = {0};
	const static int sessionIdLen = sizeof(sessionId);

	if (sessionId != gp_session_id)
	{
		sessionId = gp_session_id;
		memcpy(currentToken, &sessionId, sessionIdLen);
		if (!pg_strong_random(currentToken + sessionIdLen,
							  ENDPOINT_TOKEN_LEN - sessionIdLen))
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("failed to generate a new random token.")));
		}
	}
	return currentToken;
#else
#error A strong random number source is needed.
#endif
}

/*
 * CreateTQDestReceiverForEndpoint - Creates a dest receiver for PARALLEL RETRIEVE
 * CURSOR
 *
 * Also creates shared memory message queue here. Set the local
 * Create TupleQueueDestReceiver base on the message queue to pass tuples to
 * retriever.
 */
DestReceiver *
CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc, const char *cursorName)
{
	shm_mq_handle *shmMqHandle;

	Assert(!activeSharedEndpoint);
	Assert(!activeDsmSeg);
	Assert(EndpointCtl.GpParallelRtrvRole == PARALLEL_RETRIEVE_SENDER);

	/* Register callback to deal with proc exit. */
	register_endpoint_callbacks();

	/*
	 * The message queue needs to be created first since the dsm_handle has to
	 * be ready when create EndpointDesc entry.
	 */
	create_and_connect_mq(tupleDesc, &activeDsmSeg, &shmMqHandle);

	/*
	 * Alloc endpoint and set it as the active one for sender.
	 */
	activeSharedEndpoint =
		alloc_endpoint(cursorName, dsm_segment_handle(activeDsmSeg));
	init_session_info_entry();

	/*
	 * Once the endpoint has been created in shared memory, send acknowledge
	 * message to QD so DECLARE PARALLEL RETRIEVE CURSOR statement can finish.
	 */
	cdb_sendAckMessageToQD(ENDPOINT_READY_ACK);
	return CreateTupleQueueDestReceiver(shmMqHandle);
}


/*
 * DestroyTQDestReceiverForEndpoint - destroy TupleQueueDestReceiver
 *
 * If the queue is large enough for tuples to send, must wait for a receiver
 * to attach the message queue before endpoint detaches the message queue.
 * Cause if the queue gets detached before receiver attaches, the queue
 * will never be attached by a receiver.
 *
 * Should also clean all other endpoint info here.
 */
void
DestroyTQDestReceiverForEndpoint(DestReceiver *endpointDest)
{
	Assert(activeSharedEndpoint);
	Assert(activeDsmSeg);

	/*
	 * wait for receiver to retrieve the first row. ackDone latch will be
	 * reset to be re-used when retrieving finished.
	 */
	wait_receiver();

	/*
	 * tqueueShutdownReceiver() (rShutdown callback) will call
	 * shm_mq_detach(), so need to call it before detach_mq(). Retrieving
	 * session will set ackDone latch again after shm_mq_detach() called here.
	 */
	(*endpointDest->rShutdown) (endpointDest);
	(*endpointDest->rDestroy) (endpointDest);

	/*
	 * Wait until all data is retrieved by receiver. This is needed because
	 * when endpoint send all data to shared message queue. The retrieve
	 * session may still not get all data from
	 */
	wait_receiver();

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	unset_endpoint_sender_pid(activeSharedEndpoint);
	LWLockRelease(ParallelCursorEndpointLock);

	/*
	 * If all data get sent, hang the process and wait for QD to close it. The
	 * purpose is to not clean up EndpointDesc entry until CLOSE/COMMIT/ABORT
	 * (i.e. ProtalCleanup get executed). So user can still see the finished
	 * endpoint status through gp_endpoints_info UDF. This is needed because
	 * pg_cusor view can still see the PARALLEL RETRIEVE CURSOR
	 */
	wait_parallel_retrieve_close();

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	free_endpoint(activeSharedEndpoint);
	LWLockRelease(ParallelCursorEndpointLock);

	activeSharedEndpoint = NULL;
	detach_mq(activeDsmSeg);
	activeDsmSeg = NULL;
	ClearParallelRtrvCursorExecRole();
}

/*
 * alloc_endpoint - Allocate an EndpointDesc entry in shared memroy.
 *
 * cursorName - the parallel retrieve cursor name.
 * dsmHandle  - dsm handle of shared memory message queue.
 */
EndpointDesc *
alloc_endpoint(const char *cursorName, dsm_handle dsmHandle)
{
	int			i;
	int			foundIdx = -1;
	EndpointDesc *ret = NULL;

	Assert(sharedEndpoints);
	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

#ifdef FAULT_INJECTOR
	/* inject fault "skip" to set end-point shared memory slot full */
	FaultInjectorType_e typeE =
	SIMPLE_FAULT_INJECTOR("endpoint_shared_memory_slot_full");

	if (typeE == FaultInjectorTypeFullMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (sharedEndpoints[i].empty)
			{
				/* pretend to set a valid endpoint */
				snprintf(sharedEndpoints[i].name, ENDPOINT_NAME_LEN, "%s",
						 DUMMY_ENDPOINT_NAME);
				snprintf(sharedEndpoints[i].cursorName, NAMEDATALEN, "%s",
						 DUMMY_CURSOR_NAME);
				sharedEndpoints[i].databaseID = MyDatabaseId;
				sharedEndpoints[i].mqDsmHandle = DSM_HANDLE_INVALID;
				sharedEndpoints[i].sessionID = gp_session_id;
				sharedEndpoints[i].userID = GetUserId();
				sharedEndpoints[i].senderPid = InvalidPid;
				sharedEndpoints[i].receiverPid = InvalidPid;
				sharedEndpoints[i].empty = false;
			}
		}
	}
	else if (typeE == FaultInjectorTypeRevertMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (endpoint_name_equals(sharedEndpoints[i].name,
									 DUMMY_ENDPOINT_NAME))
			{
				sharedEndpoints[i].mqDsmHandle = DSM_HANDLE_INVALID;
				sharedEndpoints[i].empty = true;
			}
		}
	}
#endif

	/* find a new slot */
	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (sharedEndpoints[i].empty)
		{
			foundIdx = i;
			break;
		}
	}

	if (foundIdx == -1)
	{
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("failed to allocate endpoint")));
	}

	generate_endpoint_name(sharedEndpoints[i].name, cursorName, gp_session_id,
						   GpIdentity.segindex);
	StrNCpy(sharedEndpoints[i].cursorName, cursorName, NAMEDATALEN);
	sharedEndpoints[i].databaseID = MyDatabaseId;
	sharedEndpoints[i].sessionID = gp_session_id;
	sharedEndpoints[i].userID = GetUserId();
	sharedEndpoints[i].senderPid = MyProcPid;
	sharedEndpoints[i].receiverPid = InvalidPid;
	sharedEndpoints[i].attachStatus = Status_Prepared;
	sharedEndpoints[i].empty = false;
	sharedEndpoints[i].mqDsmHandle = dsmHandle;
	OwnLatch(&sharedEndpoints[i].ackDone);
	ret = &sharedEndpoints[i];

	LWLockRelease(ParallelCursorEndpointLock);
	return ret;
}

/*
 * Create and setup the shared memory message queue.
 *
 * Create a dsm which contains a TOC(table of content). It has 3 parts:
 * 1. Tuple's TupleDesc length.
 * 2. Tuple's TupleDesc.
 * 3. Shared memory message queue.
 */
void
create_and_connect_mq(TupleDesc tupleDesc, dsm_segment **mqSeg /* out */ ,
					  shm_mq_handle **mqHandle /* out */ )
{
	shm_toc    *toc;
	shm_mq	   *mq;
	shm_toc_estimator tocEst;
	Size		tocSize;
	int			tupdescLen;
	char	   *tupdescSer;
	char	   *tdlenSpace;
	char	   *tupdescSpace;
	TupleDescNode *node = makeNode(TupleDescNode);

	Assert(Gp_role == GP_ROLE_EXECUTE);

	elog(DEBUG3,
		 "CDB_ENDPOINTS: create and setup the shared memory message queue.");

	/* Serialize TupleDesc */
	node->natts = tupleDesc->natts;
	node->tuple = tupleDesc;
	tupdescSer =
		serializeNode((Node *) node, &tupdescLen, NULL /* uncompressed_size */ );

	/*
	 * Calculate dsm size, size = toc meta + toc_nentry(3) * entry size +
	 * tuple desc length size + tuple desc size + queue size.
	 */
	shm_toc_initialize_estimator(&tocEst);
	shm_toc_estimate_chunk(&tocEst, sizeof(tupdescLen));
	shm_toc_estimate_chunk(&tocEst, tupdescLen);
	shm_toc_estimate_keys(&tocEst, 2);

	shm_toc_estimate_chunk(&tocEst, ENDPOINT_TUPLE_QUEUE_SIZE);
	shm_toc_estimate_keys(&tocEst, 1);
	tocSize = shm_toc_estimate(&tocEst);

	*mqSeg = dsm_create(tocSize, 0);
	if (*mqSeg == NULL)
	{
		ereport(
			ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
			errmsg("failed to create shared message queue for endpoints.")));
	}
	dsm_pin_mapping(*mqSeg);

	toc = shm_toc_create(ENDPOINT_MSG_QUEUE_MAGIC, dsm_segment_address(*mqSeg),
						 tocSize);

	tdlenSpace = shm_toc_allocate(toc, sizeof(tupdescLen));
	memcpy(tdlenSpace, &tupdescLen, sizeof(tupdescLen));
	shm_toc_insert(toc, ENDPOINT_KEY_TUPLE_DESC_LEN, tdlenSpace);

	tupdescSpace = shm_toc_allocate(toc, tupdescLen);
	memcpy(tupdescSpace, tupdescSer, tupdescLen);
	shm_toc_insert(toc, ENDPOINT_KEY_TUPLE_DESC, tupdescSpace);

	mq = shm_mq_create(shm_toc_allocate(toc, ENDPOINT_TUPLE_QUEUE_SIZE),
					   ENDPOINT_TUPLE_QUEUE_SIZE);
	shm_toc_insert(toc, ENDPOINT_KEY_TUPLE_QUEUE, mq);
	shm_mq_set_sender(mq, MyProc);
	*mqHandle = shm_mq_attach(mq, *mqSeg, NULL);
}

/*
 * init_session_info_entry.
 *
 * Create/reuse SessionInfoEntry for current session in shared memory.
 * SessionInfoEntry is used for retrieve auth.
 */
void
init_session_info_entry(void)
{
	SessionInfoEntry	*infoEntry = NULL;
	bool				 found = false;
	SessionTokenTag		 tag;
	const int8			*token = NULL;

	tag.sessionID	= gp_session_id;
	tag.userID		= GetUserId();

	/* track current session id for session_info_clean_callback  */
	EndpointCtl.sessionID = gp_session_id;

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	infoEntry = (SessionInfoEntry *) hash_search(sharedSessionInfoHash, &tag,
												 HASH_ENTER, &found);
	elog(DEBUG3, "CDB_ENDPOINT: Finish endpoint init. Found SessionInfoEntry: %d",
		 found);
	/*
	 * Save the token if it is the first time we create endpoint in current
	 * session. We guarantee that one session will map to one token only.
	 */
	if (!found)
	{
		/* Track userID in current transaction */
		MemoryContext oldMemoryCtx = MemoryContextSwitchTo(TopMemoryContext);
		sessionUserList = lappend_oid(sessionUserList, GetUserId());
		MemoryContextSwitchTo(oldMemoryCtx);

		token = get_or_create_token();
		memcpy(infoEntry->token, token, ENDPOINT_TOKEN_LEN);
		infoEntry->endpointCounter = 0;
	}
	infoEntry->endpointCounter++;
	/*
	 * Overwrite exists token in case the wrapped session id entry not get
	 * removed For example, 1 hours ago, a session 7 exists and have entry
	 * with token 123. And for some reason the entry not get remove by
	 * session_info_clean_callback. Now current session is session 7 again.
	 * Here need to overwrite the old token.
	 */
	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * wait_receiver - wait receiver to retrieve at least once from the
 * shared memory message queue.
 *
 * If the queue only attached by the sender and the queue is large enough
 * for all tuples, sender should wait receiver. Cause if sender detached
 * from the queue, the queue will be not available for receiver.
 */
void
wait_receiver(void)
{
	elog(DEBUG3, "CDB_ENDPOINTS: wait receiver.");
	while (true)
	{
		int			wr;

		CHECK_FOR_INTERRUPTS();

		if (QueryFinishPending)
			break;

		check_dispatch_connection();

		elog(DEBUG5, "CDB_ENDPOINT: sender wait latch in wait_receiver()");
		wr = WaitLatch(&activeSharedEndpoint->ackDone,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   WAIT_NORMAL_TIMEOUT);
		if (wr & WL_TIMEOUT)
			continue;

		if (wr & WL_POSTMASTER_DEATH)
		{
			endpoint_abort();
			elog(DEBUG3, "CDB_ENDPOINT: postmaster exit, close shared memory "
				 "message queue.");
			proc_exit(0);
		}

		Assert(wr & WL_LATCH_SET);
		elog(DEBUG3, "CDB_ENDPOINT:sender reset latch in wait_receiver()");
		ResetLatch(&activeSharedEndpoint->ackDone);
		break;
	}
}

/*
 * Detach the shared memory message queue.
 * This should happen after free endpoint, otherwise endpoint->mq_dsm_handle
 * becomes invalid pointer.
 */
void
detach_mq(dsm_segment *dsmSeg)
{
	elog(DEBUG3, "CDB_ENDPOINT: Sender message queue detaching. '%p'",
		 (void *) dsmSeg);

	Assert(dsmSeg);
	dsm_detach(dsmSeg);
}

/*
 * Unset endpoint sender pid.
 *
 * Clean the EndpointDesc entry sender pid when endpoint finish it's
 * job or abort.
 * Needs to be called with exclusive lock on ParallelCursorEndpointLock.
 */
void
unset_endpoint_sender_pid(volatile EndpointDesc * endPointDesc)
{
	SessionInfoEntry *sessionInfoEntry;
	SessionTokenTag tag;

	tag.sessionID = gp_session_id;
	tag.userID = GetUserId();

	if (!endPointDesc || endPointDesc->empty)
	{
		return;
	}
	elog(DEBUG3, "CDB_ENDPOINT: unset endpoint sender pid.");

	/*
	 * Since the receiver is not in the session, sender has the duty to cancel
	 * it.
	 */
	signal_receiver_abort(endPointDesc->receiverPid,
						  endPointDesc->attachStatus);

	/*
	 * Only the endpoint QE/entry DB execute this unset sender pid function. The
	 * sender pid in Endpoint entry must be MyProcPid or InvalidPid.
	 */
	Assert(MyProcPid == endPointDesc->senderPid ||
		   endPointDesc->senderPid == InvalidPid);
	if (MyProcPid == endPointDesc->senderPid)
	{
		endPointDesc->senderPid = InvalidPid;
		sessionInfoEntry =
			hash_search(sharedSessionInfoHash, &tag, HASH_FIND, NULL);

		/* sessionInfoEntry may get removed. This means xact finished. */
		if (sessionInfoEntry)
		{
			cdb_sendAckMessageToQD(ENDPOINT_FINISHED_ACK);
		}
	}
}

/*
 * If endpoint exit with error, let retrieve role know exception happened.
 * Called by endpoint.
 */
void
signal_receiver_abort(pid_t receiverPid, enum AttachStatus attachStatus)
{
	bool		isAttached;

	elog(DEBUG3, "CDB_ENDPOINT: signal the receiver to abort.");

	isAttached = (attachStatus == Status_Attached);
	if (receiverPid != InvalidPid && isAttached && receiverPid != MyProcPid)
	{
		SetBackendCancelMessage(receiverPid, "Signal the receiver to abort.");
		kill(receiverPid, SIGINT);
	}
}

/*
 * endpoint_abort - xact abort routine for endpoint
 */
void
endpoint_abort(void)
{
	if (activeSharedEndpoint)
	{
		LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
		/*
		 * These two better be called in one lock section.
		 * So retriever abort will not execute extra works.
		 */
		unset_endpoint_sender_pid(activeSharedEndpoint);
		free_endpoint(activeSharedEndpoint);
		LWLockRelease(ParallelCursorEndpointLock);
		activeSharedEndpoint = NULL;
	}

	/*
	 * During xact abort, should make sure the endpoint_cleanup called first.
	 * Cause if call detach_mq to detach the message queue first, the
	 * retriever may read NULL from message queue, then retrieve mark itself
	 * down.
	 *
	 * So here, need to make sure signal retrieve abort first before endpoint
	 * detach message queue.
	 */
	if (activeDsmSeg)
	{
		detach_mq(activeDsmSeg);
		activeDsmSeg = NULL;
	}
	ClearParallelRtrvCursorExecRole();
}

/*
 * Wait for PARALLEL RETRIEVE CURSOR cleanup after endpoint send all data.
 *
 * If all data get sent, hang the process and wait for QD to close it.
 * The purpose is to not clean up EndpointDesc entry until
 * CLOSE/COMMIT/ABORT (ie. ProtalCleanup get executed).
 */
void
wait_parallel_retrieve_close(void)
{
	ResetLatch(&MyProc->procLatch);
	while (true)
	{
		int			wr;

		CHECK_FOR_INTERRUPTS();

		if (QueryFinishPending)
			break;

		check_dispatch_connection();

		elog(DEBUG5, "CDB_ENDPOINT: wait for parallel retrieve cursor close");
		wr = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   WAIT_NORMAL_TIMEOUT);
		if (wr & WL_TIMEOUT)
			continue;

		if (wr & WL_POSTMASTER_DEATH)
		{
			elog(DEBUG3, "CDB_ENDPOINT: postmaster exit, close shared memory "
				 "message queue.");
			proc_exit(0);
		}

		Assert(wr & WL_LATCH_SET);
		elog(DEBUG3, "CDB_ENDPOINT: parallel retrieve cursor close, get latch");
		ResetLatch(&MyProc->procLatch);
		break;
	}
}

/*
 * free_endpoint - Frees the given endpoint.
 *
 * Needs to be called with exclusive lock on ParallelCursorEndpointLock.
 */
void
free_endpoint(volatile EndpointDesc * endpoint)
{
	SessionTokenTag tag;
	SessionInfoEntry *infoEntry = NULL;

	Assert(endpoint);
	Assert(!endpoint->empty);

	elog(DEBUG3, "CDB_ENDPOINTS: Free endpoint '%s'.", endpoint->name);

	endpoint->databaseID = InvalidOid;
	endpoint->mqDsmHandle = DSM_HANDLE_INVALID;
	endpoint->sessionID = InvalidSession;
	endpoint->userID = InvalidOid;
	endpoint->empty = true;
	memset((char *) endpoint->name, '\0', ENDPOINT_NAME_LEN);
	ResetLatch(&endpoint->ackDone);
	DisownLatch(&endpoint->ackDone);

	tag.sessionID = endpoint->sessionID;
	tag.userID = endpoint->userID;
	infoEntry = (SessionInfoEntry *) hash_search(
		sharedSessionInfoHash, &tag, HASH_FIND, NULL);
	Assert(infoEntry);
	Assert(infoEntry->endpointCounter > 0);
	if (infoEntry) {
		infoEntry->endpointCounter--;
	}
}

/*
 * Register callback for endpoint/sender to deal with xact abort.
 */
static void
register_endpoint_callbacks(void)
{
	static bool isRegistered = false;

	if (!isRegistered)
	{
		/* Register callback to deal with proc endpoint xact abort. */
		RegisterSubXactCallback(sender_subxact_callback, NULL);
		RegisterXactCallback(sender_xact_abort_callback, NULL);

		RegisterXactCallback(session_info_clean_callback, NULL);
		isRegistered = true;
	}
}

/*
 * If endpoint/sender on xact abort, do endpoint clean jobs.
 */
void
sender_xact_abort_callback(XactEvent ev, void *vp)
{
	if (ev == XACT_EVENT_ABORT || ev == XACT_EVENT_PARALLEL_ABORT)
	{
		if (Gp_role == GP_ROLE_RETRIEVE || Gp_role == GP_ROLE_UTILITY)
		{
			return;
		}
		elog(DEBUG3, "CDB_ENDPOINT: sender xact abort callback");
		endpoint_abort();
	}
}

/*
 * If endpoint/sender on sub xact abort, do endpoint clean jobs.
 */
void
sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		sender_xact_abort_callback(XACT_EVENT_ABORT, arg);
	}
}

EndpointDesc *
get_endpointdesc_by_index(int index)
{
	Assert(sharedEndpoints);
	Assert(index > -1 && index < MAX_ENDPOINT_SIZE);
	return &sharedEndpoints[index];
}

/*
 *
 * find_endpoint - Find the endpoint by given endpoint name and session id.
 *
 * For the endpoint, the session_id is the gp_session_id since it is the same
 * with the session which created the parallel retrieve cursor.
 * For the retriever, the session_id is picked by the token when doing the
 * authentication.
 *
 * The caller is responsible for acquiring ParallelCursorEndpointLock lock.
 */
EndpointDesc *
find_endpoint(const char *endpointName, int sessionID)
{
	EndpointDesc *res = NULL;

	Assert(endpointName);

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		/* endpoint_name is unique across sessions. But it is not right, */
		/* need to fetch the endpoint created in the session with the */
		/* given session_id. */
		if (!sharedEndpoints[i].empty &&
			sharedEndpoints[i].sessionID == sessionID &&
			endpoint_name_equals(sharedEndpoints[i].name, endpointName) &&
			sharedEndpoints[i].databaseID == MyDatabaseId)
		{
			res = &sharedEndpoints[i];
			break;
		}
	}

	return res;
}

/*
 * get_token_by_session_id - get token based on given session id and user.
 */
void
get_token_by_session_id(int sessionId, Oid userID, int8 *token /* out */ )
{
	SessionInfoEntry *infoEntry = NULL;
	SessionTokenTag tag;

	tag.sessionID = sessionId;
	tag.userID = userID;

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	infoEntry = (SessionInfoEntry *) hash_search(sharedSessionInfoHash, &tag,
												 HASH_FIND, NULL);
	if (infoEntry == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("token for user id: %d, session: %d doesn't exist",
							   tag.userID, sessionId)));
	}
	memcpy(token, infoEntry->token, ENDPOINT_TOKEN_LEN);
	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * get_session_id_for_auth - Find the corresponding session id by the given token.
 */
int
get_session_id_for_auth(Oid userID, const int8 *token)
{
	int			sessionId = InvalidSession;
	SessionInfoEntry *infoEntry = NULL;
	HASH_SEQ_STATUS status;

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	hash_seq_init(&status, sharedSessionInfoHash);
	while ((infoEntry = (SessionInfoEntry *) hash_seq_search(&status)) != NULL)
	{
		if (token_equals(infoEntry->token, token) &&
			userID == infoEntry->tag.userID)
		{
			sessionId = infoEntry->tag.sessionID;
			hash_seq_term(&status);
			break;
		}
	}
	LWLockRelease(ParallelCursorEndpointLock);

	return sessionId;
}

/*
 * generate_endpoint_name
 *
 * Generate the endpoint name based on the PARALLEL RETRIEVE CURSOR name,
 * session ID and the segment index.lwlock.hlwlock.h
 * The endpoint name should be unique across sessions.
 */
void
generate_endpoint_name(char *name, const char *cursorName, int32 sessionID,
					   int32 segindex)
{
	/*
	 * Use counter to avoid duplicated endpoint names when error happens.
	 * Since the retrieve session won't be terminated when transaction abort,
	 * reuse the previous endpoint name may cause unexpected behavior for the
	 * retrieving session.
	 */
	static uint8 counter = 0;

	snprintf(name, ENDPOINT_NAME_LEN, "%s%08x%08x%02x", cursorName, sessionID,
			 segindex, counter++);
}

/*
 * Check the QD dispatcher connection whether is lost
 */
void
check_dispatch_connection(void)
{
	unsigned char firstchar;
	int			r;

	pq_startmsgread();
	r = pq_getbyte_if_available(&firstchar);
	if (r < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("unexpected EOF on query dispatcher connection")));
	}
	else if (r > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						errmsg("query dispatcher should get nothing until QE "
							   "backend finished processing")));
	}
	else
	{
		/* no data available without blocking */
		pq_endmsgread();
		/* continue processing as normal case */
	}
}

/*
 * session_info_clean_callback - callback when xact finished
 *
 * When an xact finished, clean "session - token" mapping entry in
 * shared memory is needed, since there's no endpoint for current session.
 */
void
session_info_clean_callback(XactEvent ev, void *vp)
{
	if ((ev == XACT_EVENT_COMMIT || ev == XACT_EVENT_PARALLEL_COMMIT ||
		 ev == XACT_EVENT_ABORT || ev == XACT_EVENT_PARALLEL_ABORT) &&
		(Gp_is_writer || Gp_role == GP_ROLE_DISPATCH))
	{
		elog(DEBUG3,
			 "CDB_ENDPOINT: session_info_clean_callback clean token for session %d",
			 EndpointCtl.sessionID);

		if (sessionUserList && sessionUserList->length > 0)
		{
			ListCell   *cell;
			LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
			foreach(cell, sessionUserList)
			{
				bool find = false;
				SessionTokenTag tag;

				/*
				 * When proc exit, the gp_session_id is -1, so use our record session
				 * id instead
				 */
				tag.sessionID = EndpointCtl.sessionID;
				tag.userID = lfirst_oid(cell);

				SessionInfoEntry *infoEntry = (SessionInfoEntry *) hash_search(
					sharedSessionInfoHash, &tag, HASH_FIND, &find);
				if (infoEntry && infoEntry->endpointCounter <= 0)
				{
					hash_search(sharedSessionInfoHash, &tag, HASH_REMOVE, &find);
					elog(DEBUG3,
						 "CDB_ENDPOINT: session_info_clean_callback removes exists entry for "
						 "user id: %d, session: %d",
						 tag.userID, EndpointCtl.sessionID);
				}
			}
			LWLockRelease(ParallelCursorEndpointLock);
			list_free(sessionUserList);
			sessionUserList = NULL;
		}
	}
}

/*
 * gp_check_parallel_retrieve_cursor
 *
 * Check whether given parallel retrieve cursor is finished immediately.
 *
 * Return true means finished.
 * Error out when parallel retrieve cursor has exception raised.
 */
Datum
gp_check_parallel_retrieve_cursor(PG_FUNCTION_ARGS)
{
	const char *cursorName = NULL;

	cursorName = PG_GETARG_CSTRING(0);

	PG_RETURN_BOOL(check_parallel_retrieve_cursor(cursorName, false));
}

/*
 * gp_check_parallel_retrieve_cursor
 *
 * Wait until given parallel retrieve cursor is finished.
 *
 * Return true means finished.
 * Error out when parallel retrieve cursor has exception raised.
 */
Datum
gp_wait_parallel_retrieve_cursor(PG_FUNCTION_ARGS)
{
	const char *cursorName = NULL;

	cursorName = PG_GETARG_CSTRING(0);

	PG_RETURN_BOOL(check_parallel_retrieve_cursor(cursorName, true));
}

/*
 * check_parallel_retrieve_cursor
 *
 * Support function for UDFs:
 * gp_check_parallel_retrieve_cursor
 * gp_wait_parallel_retrieve_cursor
 *
 * Check whether given parallel retrieve cursor is finished.
 * If isWait is true, hang until parallel retrieve cursor finished.
 *
 * Return true means finished.
 * Error out when parallel retrieve cursor has exception raised.
 */
bool
check_parallel_retrieve_cursor(const char *cursorName, bool isWait)
{
	bool		retVal = false;
	bool		isParallelRetrieve = false;
	Portal		portal;
	EState *estate = NULL;

	/* get the portal from the portal name */
	portal = GetPortalByName(cursorName);
	if (!PortalIsValid(portal))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR),
						errmsg("cursor \"%s\" does not exist", cursorName)));
		return false;			/* keep compiler happy */
	}
	isParallelRetrieve =
		(portal->cursorOptions & CURSOR_OPT_PARALLEL_RETRIEVE) > 0;
	if (!isParallelRetrieve)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
			   errmsg("this UDF only works for PARALLEL RETRIEVE CURSOR.")));
		return false;
	}
	estate = portal->queryDesc->estate;
	retVal = cdbdisp_waitDispatchAckMessage(estate->dispatcherState, ENDPOINT_FINISHED_ACK, isWait);

#ifdef FAULT_INJECTOR
	HOLD_INTERRUPTS();
	SIMPLE_FAULT_INJECTOR("check_parallel_retrieve_cursor_after_udf");
	RESUME_INTERRUPTS();
#endif

	check_parallel_cursor_errors(estate);
	return retVal;
}

/*
 * check_parallel_cursor_errors - Check the PARALLEL RETRIEVE CURSOR execution
 * status
 *
 * If get error, then rethrow the error.
 * Return true if the PARALLEL RETRIEVE CURSOR Execution Finished.
 */
void
check_parallel_cursor_errors(EState *estate)
{
	CdbDispatcherState *ds;

	Assert(estate);

	ds = estate->dispatcherState;
	/*
	 * If QD, wait for QEs to finish and check their results.
	 */
	if (cdbdisp_checkResultsErrcode(ds->primaryResults))
	{
		ErrorData  *qeError = NULL;

		cdbdisp_getDispatchResults(ds, &qeError);
		Assert(qeError);
		estate->dispatcherState = NULL;
		cdbdisp_cancelDispatch(ds);
		cdbdisp_destroyDispatcherState(ds);
		ReThrowError(qeError);
	}
}
