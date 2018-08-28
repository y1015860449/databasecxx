#include"mongodbManager.h"

CMongodbConnection::CMongodbConnection()
{
}

CMongodbConnection::~CMongodbConnection()
{
}

void CMongodbConnection::setClient(mongoc_client_t* pCli) 
{ 
	if (nullptr != pCli) 
		m_pClient = pCli; 
}

mongoc_client_t* CMongodbConnection::getClient()
{
	if (nullptr != m_pClient) 
		return m_pClient;
       return nullptr;	
}

bool CMongodbConnection::InsertDoc(const char*db, const char*tb, const bson_t *doc)
{
	mongoc_collection_t *collection = mongoc_client_get_collection(m_pClient, db, tb);
       	if(nullptr == collection)
       	{
 		return false;
       	}	       
	if (!mongoc_collection_insert(collection, MONGOC_INSERT_NONE, doc, nullptr, &m_Error_t))
	{
		char *docStr = bson_as_json(doc, nullptr);
		bson_free(docStr);
		mongoc_collection_destroy(collection);
		return false;
	}
	mongoc_collection_destroy(collection);
	return true;
}

mongoc_cursor_t* CMongodbConnection::SearchDocs(const char*db, const char*tb, const bson_t *cond, 
		bool& bResult, unsigned int skip, unsigned int limit, unsigned int batch_size, const bson_t *fields)
{
	bResult = false;
	mongoc_collection_t *collection = mongoc_client_get_collection(m_pClient, db, tb);
	if(nullptr == collection)
	{
		return nullptr;
	}

	mongoc_cursor_t* cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, skip, limit, batch_size, cond, fields, nullptr);
	if (nullptr == cursor)
	{
		char *condStr = bson_as_json(cond, nullptr);
		bson_free(condStr);
		mongoc_collection_destroy(collection);
		return nullptr;
	}
	mongoc_collection_destroy(collection);
	bResult = true;
	return cursor;
}


bool CMongodbConnection::UpdateDoc(const char*db, const char*coll, const bson_t *cond, const bson_t *updatedoc, bool isUpSert)
{
	mongoc_collection_t *collection = mongoc_client_get_collection(m_pClient, db, coll); 
	if (nullptr == collection) 
	{
		return false;
	}

	mongoc_update_flags_t updateFlag = MONGOC_UPDATE_MULTI_UPDATE;
	if(isUpSert)
		updateFlag = MONGOC_UPDATE_UPSERT;

	if (!mongoc_collection_update(collection, updateFlag, cond, updatedoc, nullptr, &m_Error_t)) //MONGOC_UPDATE_MULTI_UPDATE更新所有符合结果的
	{
		char *condStr = bson_as_json(cond, nullptr);
		bson_free(condStr);
		mongoc_collection_destroy(collection);
		return false;
	}
	mongoc_collection_destroy(collection);
	return true;
}

bool CMongodbConnection::DeleteDoc(const char*db, const char*coll, const bson_t *cond, bool isAll)
{
	mongoc_collection_t *collection = mongoc_client_get_collection(m_pClient, db, coll); //指定数据库和集合
	if (nullptr == collection)
	{
		return false;
	}
	mongoc_remove_flags_t removeFlag = MONGOC_REMOVE_SINGLE_REMOVE;
	if (isAll)
		removeFlag = MONGOC_REMOVE_NONE;
	if (!mongoc_collection_remove(collection, removeFlag, cond, nullptr, &m_Error_t))
	{
		char *condStr = bson_as_json(cond, nullptr);
		bson_free(condStr);
		mongoc_collection_destroy(collection);
		return false;
	}
	mongoc_collection_destroy(collection);
	return true;
}

int CMongodbConnection::GetCount(const char*db, const char*coll, const bson_t *cond, bool& bResult)
{
		
	bResult = false;
	mongoc_collection_t *collection = mongoc_client_get_collection(m_pClient, db, coll);
	if (!collection)
	{
		return -1;
	}
	int nCount = mongoc_collection_count(collection, MONGOC_QUERY_NONE, cond, 0, 0, NULL, &m_Error_t);
	if (nCount < 0)
	{
		char *condStr = bson_as_json(cond, NULL);
		bson_free(condStr);
		mongoc_collection_destroy(collection);
		return -1;
	}
	bResult = true;
	mongoc_collection_destroy(collection);
	return nCount;
}










CMongodbPool::CMongodbPool(const std::string& strPoolName, const std::string& strHost, int  nPort, int nMinCon, int nMaxCon)
	: m_strPoolName(strPoolName)
	, m_strIp(strHost)
	, m_nPort(nPort)
	, m_nMinConn(nMinCon)
	, m_nMaxConn(nMaxCon)
	, m_nCurConn(0)
	, m_pUri(nullptr)
	, m_pMongocPool(nullptr)
{

}

CMongodbPool::~CMongodbPool()
{
	if (nullptr != m_pMongocPool)
	{
		mongoc_client_pool_destroy(m_pMongocPool);
		m_pMongocPool = nullptr;
	}

	for (auto it : m_listConn)
	{
		delete it;
		it = nullptr;
	}
	m_listConn.clear();
	mongoc_cleanup();
}

bool CMongodbPool::initMongodbPool()
{
	mongoc_init();
	std::string strUri = "mongodb://";
	//strUri = strUri + m_strIp + ":" + std::to_string(m_nPort) + "/?minPoolSize=" + std::to_string(m_nMinConn)  + "&maxPoolSize=" + std::to_string(m_nMaxConn);	
	strUri = strUri + m_strIp + ":" + std::to_string(m_nPort);
	if (nullptr == m_pUri)
	{
		m_pUri = mongoc_uri_new(strUri.c_str());
		if (nullptr == m_pUri)
			return false;
	}
	if (nullptr == m_pMongocPool)
	{
		m_pMongocPool = mongoc_client_pool_new(m_pUri);
		if (nullptr == m_pMongocPool)
			return false;
	}
	//mongoc_client_pool_min_size(m_pMongocPool, m_nMinConn);
	//mongoc_client_pool_max_size(m_pMongocPool, m_nMaxConn);

	for (int i = 0;i < m_nMinConn;++i)
	{
		CMongodbConnection* pConn = new CMongodbConnection();
		m_listConn.push_back(pConn);
	}
	m_nCurConn = m_nMinConn.load(std::memory_order_relaxed);
	return true;
}


CMongodbConnection*  CMongodbPool::getMongoConnection()
{
	CMongodbConnection* pConn = nullptr;
	std::unique_lock<std::mutex> lck(m_mtx);
	while (m_listConn.empty())
	{
		if (m_nCurConn < m_nMaxConn)
		{
			CMongodbConnection* pConnN = new CMongodbConnection();
			m_listConn.push_back(pConnN);
			m_nCurConn++;
		}
		else
		{
			m_condv.wait(lck, [=]()->bool { return !m_listConn.empty(); });
		}
	}

	mongoc_client_t* pCli = mongoc_client_pool_pop(m_pMongocPool);
	if (nullptr != pCli)
	{
		pConn = m_listConn.front();
		m_listConn.pop_front();
		pConn->setClient(pCli);
	}

	return pConn;
}

void CMongodbPool::relMonogoConnect(CMongodbConnection* conn)
{
	if (nullptr != conn)
	{
		m_listConn.push_back(conn);
		if (nullptr != conn->getClient())
			mongoc_client_pool_push(m_pMongocPool, conn->getClient());
		std::unique_lock<std::mutex> lck(m_mtx);
		m_condv.notify_all();
	}
}
