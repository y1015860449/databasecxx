// mongodbcxx.cpp : ¶¨Òå¿ØÖÆÌ¨Ó¦ÓÃ³ÌÐòµÄÈë¿Úµã¡£
//

#include"mongodbManager.h"
#include<thread>

int main()
{
	//CMongodbPool* pPool = new CMongodbPool("msgSvr", "192.168.1.98", 30000, 15,50 );
	CMongodbPool* pPool = new CMongodbPool("msgSvr", "127.0.0.1",27017 , 15,50 );
	pPool->initMongodbPool();
	for (int j = 0; j <40 ; j++)
	{
		std::thread thr([pPool, j] {//, "fromId", BCON_UTF8("4071468")
			CMongodbConnection*  pConn = pPool->getMongoConnection();
			bson_t* pBson = BCON_NEW("msgId", BCON_UTF8("339f9fcaa82f4336b94462ef9df9155d")
						, "toId", BCON_UTF8("4071572"), "cmdId", BCON_INT32(45062), "msgData", BCON_UTF8("\n\u00074071468\u0012\u00071450860\u001a 329D5A9D01A44079B93B3D4FEE68FA11 \u0001(¿¿¿¿¿¿¿0\u0001:¿¿u0003{\n  \"secretKey\" : \"koKe5st8WRhFNHnbQXXkiDSqaxscMdw2fxiguiowtUzZcKQlB\\/NGFHi4QPllMODubQkSpGnPOKb4Jer+4XpMwLXPaGCSkYm9wabjyMRDJepWWq6BNy\\/qVP8oWy1rJtJ78zO\\/Ufa9XYV79Hs7+pqGxabyPrOBwOprVsaiCS2GkUCIaqpxUB1W6e4k3za8q7NhtTtUZ6A1PGS3jZeAb5mT+nScIon\\/yMNub3GnGSzR6itK\\/ez9BDVmV0l5Gg+8ZGWNaEkBnwyOw8SShpEEOYtI6o30uAgs510UjFNrAt2aQmNxkcK3TGCMDuyFaMguu+8XezCVD3CuLc1sa7x3vWTtFA==\",\n  \"cipherText\" : \"HGIdbkfKpSgrmil1HERbunrL0aC3RpaFpRtZSN4Qc3sG2ldgWFMCwkNgkJMU1Jbb\"\n}"),"createTime", BCON_INT64(1533887669255), "isChatDeliver", BCON_INT32(0), "bPulled", BCON_INT32(1));
			struct timeval tv;
			struct timezone tz;
			int index = 100000 * j;
			//std::string strColl = "test_coll_" + std::to_string(j);
			std::string strColl = "test_coll";
			std::string strDb = "shawdb";
			for (int i = 0; i < 100000; i++)
			{
				BSON_APPEND_UTF8(pBson, "fromId", std::to_string(index + i).c_str());
				//CMongodbConnection*  pConn = pPool->getMongoConnection();
				//if(pConn->InsertDoc("msgSvr", "offlineMsg", pBson))
				if(pConn->InsertDoc(strDb.c_str(), strColl.c_str(), pBson))
				{
					gettimeofday(&tv, &tz);
					printf("thread:%d    %ld.%ld \n", j, tv.tv_sec, tv.tv_usec / 1000);
				}
				//pPool->relMonogoConnect(pConn);
			}
			pPool->relMonogoConnect(pConn);
			bson_destroy(pBson);	
		});
		thr.detach();
	}

	//for (int i = 0; i < 40;++i)
	//{
	//	std::thread th([] {
	//		while (1)
	//		{
	//			usleep(10);
	//		}
	//	});
	//	th.detach();
	//}

	while(1);

	delete pPool;
	return 0;
}

