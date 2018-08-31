#ifndef __MONGODB_MANAGER_H__
#define __MONGODB_MANAGER_H__

#include<string>
#include"mongoc.h"
#include<list>
#include<atomic>
#include<mutex>
#include<condition_variable>


class CMongodbConnection
{
public:
	CMongodbConnection();
	~CMongodbConnection();

	void setClient(mongoc_client_t* pCli);
	mongoc_client_t* getClient();


	bool InsertDoc(const char*db, const char*tb, const bson_t *document, const bson_t *opts = nullptr, bson_t *reply = nullptr, size_t n_documents = 0, bool isMany = false);
	mongoc_cursor_t* SearchDocs(const char*db, const char*tb, const bson_t *cond, bool& bResult, unsigned int skip, unsigned int limit, unsigned int batch_size, const bson_t *fields);
	bool UpdateDoc(const char*db, const char*coll, const bson_t *cond, const bson_t *updatedoc, bool isUpSert = false);
	bool DeleteDoc(const char*db, const char*coll, const bson_t *cond, bool isAll = false);
	int GetCount(const char*db, const char*coll, const bson_t *cond, bool& bResult);

private:
	mongoc_client_t* m_pClient;
	bson_error_t m_Error_t;
};


class CMongodbPool
{
public:
	CMongodbPool(const std::string& strPoolName, const std::string& strHost, int  nPort, int nMinCon, int nMaxCon);
	~CMongodbPool();

	bool initMongodbPool();

	CMongodbConnection*  getMongoConnection();
	void relMonogoConnect(CMongodbConnection* conn);


private:
	std::string m_strPoolName;
	std::string m_strIp;
	int m_nPort;
	std::atomic<int> m_nMinConn;
	std::atomic<int> m_nMaxConn;
	std::atomic<int> m_nCurConn;
	mongoc_uri_t* m_pUri;
	mongoc_client_pool_t* m_pMongocPool;
	std::list<CMongodbConnection*> m_listConn;

	std::condition_variable m_condv;
	std::mutex m_mtx;
};

#endif // !__MONGODB_MANAGER_H__
