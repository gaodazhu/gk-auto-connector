/**
 * Thrift files can namespace, package, or prefix their output in various
 * target languages.
 */
namespace cpp com.mlsc
namespace java com.mlsc
namespace csharp com.mlsc

/**
 * send message failed exception
 */
exception SendFailException {
  1: string errorMsg
}

service MetaqService{
	/**
	 * send message as json string 
	 * eg:{ 
	 *		time:'2012-01-02 01:01:23'
	 *		points:[
	 *			point1 : 0.1545
	 *			point2 : 2.1445
	 *			point3 : 4.2345
	 *		    ...
	 *			pointn : 0.45645
	 *		]
	 *    }
	 */
	bool sendMsg(1:string jsonMsg) throws (1:SendFailException ex),
}
