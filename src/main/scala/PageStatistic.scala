import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import net.sf.json.util.JSONUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable


/**
  * Created by MiYang on 2018/6/27 15:17.
  */
object PageStatistic {


  def main(args: Array[String]): Unit = {
    // 获取任务限制参数
    //获取json字符串
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    //taskParam限制参数的,是JSON格式的,json对象
    val taskParam = JSONObject.fromObject(jsonStr)

    // 创建主键，用于存储到MySQL时传入，作为主键
    val taskUUID = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setAppName("page").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取配置文件中的pageFlow
    // pageFlow: String  1,2,3,4,5,6,7
    val pageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)

    // pageFlowArray：Array[String] (1,2,3,4,5,6,7)
    val pageFlowArray = pageFlow.split(",")
    // pageFlowArray.slice(0, pageFlowArray.length - 1)：(1,2,3,4,5,6)
    // pageFlowArray.tail: (2,3,4,5,6,7)
    // zip: (1,2) (2,3) ....
    // targetPageSplit: Array[String]  (1_2,2_3,....)
    val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }

    // 原始的符合过滤条件的数据 sessionId2Action: (sessionId, UserVisitAciton)
    val sessionId2ActionRDD = getSessionAction(sparkSession, taskParam)

    //聚合数据，斧头形式的 sessionid2GroupRDD: (sessionId, iterable[UserVisitAction])
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    //按时间排序
    // pageSplitRDD：[(String, Long)]   (page1_page2, 1L), (page2_page3, 1L)
    val pageSplitRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val sortedAction = iterableAction.toList.sortWith((action1, action2) =>
          DateUtils.parseTime(action1.action_time).getTime <
            DateUtils.parseTime(action2.action_time).getTime
        )

        // pageFlow: page1, page2, page3....
        val pageFlow = sortedAction.map(item => item.page_id)
        // pageSplit: List[String] (page1_page2, page2_page3.....)
        val pageSplit = pageFlow.slice(0, pageFlow.length - 1).zip(pageFlow.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }

        val pageFilteredSplit = pageSplit.filter(item => targetPageSplit.contains(item)).map {
          item => (item, 1L)
        }

        pageFilteredSplit
    }

    // pageCountMap: HashMap[(String, Long)]   (page1_page2, count)\
    val pageCountMap = pageSplitRDD.countByKey() //返回map

    val startPage = pageFlowArray(0).toLong
    val startPageCount = sessionId2ActionRDD.filter {
      case (sessionId, action) =>
        action.page_id == startPage
    }.count()

    // pageConvertMap：用来保存每一个切片对应的转化率
    val pageConvertMap = new mutable.HashMap[String, Double]()
    //用于做分母，指上一个的总数count，例如1_2 / 2_3 ，2_3 / 3_4  则指2_3
    var lastCount = startPageCount.toDouble

    for (pageSplit <- targetPageSplit) {
      //用于做分子，例如 1_2 / 2_3  则指1_2
      val currentCount = pageCountMap(pageSplit).toDouble
      val rate = currentCount / lastCount
      pageConvertMap += (pageSplit -> rate)
      //更新分母，即上一个的分子
      lastCount = currentCount
    }

    // pageConvertMap.map => seq[String] (pageSplit=rate, pageSplit=rate)
    // mkString("|"): pageSplit=rate|pageSplit=rate|pageSplit=rate|.....
    val rateStr = pageConvertMap.map{
      case(pageSplit,rate)=>pageSplit+"="+rate
    }.mkString("|")
    //PageSplitConvertRate定义好的样例类
    val pageSplit = PageSplitConvertRate(taskUUID,rateStr)

    //转化成RDD
    val pageSplitNewRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))

    //存入MySQL
    import sparkSession.implicits._
    pageSplitNewRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate0115")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()


  }

  def getSessionAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >='" + startDate + "'and date<='" + endDate + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }

}
