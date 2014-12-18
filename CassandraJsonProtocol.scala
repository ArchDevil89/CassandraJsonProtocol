import com.datastax.driver.core._
import spray.json._
import spray.http._
import spray.json.DefaultJsonProtocol._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling._

object CassandraTypes {

  val CassandraAscii = classOf[java.lang.String]
  val CassandraBigInt = classOf[java.lang.Long]
  val CassandraBlob = classOf[java.nio.ByteBuffer] //TODO
  val CassandraBoolean = classOf[java.lang.Boolean]
  val CassandraCounter = classOf[java.lang.Long]
  val CassandraDecimal = classOf[java.math.BigDecimal]
  val CassandraDouble = classOf[java.lang.Double]
  val CassandraFloat = classOf[java.lang.Float]
  val CassandraInet = classOf[java.net.InetAddress]
  val CassandraInt = classOf[java.lang.Integer]
  val CassandraList = classOf[java.util.List[Any]]
  val CassandraMap = classOf[java.util.Map[Any, Any]]
  val CassandraSet = classOf[java.util.Set[Any]]
  val CassandraText = classOf[java.lang.String]
  val CassandraTimestamp = classOf[java.util.Date]
  val CassandraTimeUUID = classOf[java.util.UUID]
  val CassandraUUID = classOf[java.util.UUID]
  val CassandraVarChar = classOf[java.lang.String]
  val CassandraVarInt = classOf[java.math.BigInteger]
  val CassandraTuple = classOf[TupleValue]

  def convertToJsValue(columnType : DataType, obj : GettableByIndexData, index: Int) : JsValue = {
    import scala.collection.JavaConversions._
    if(obj.isNull(index) && !columnType.isCollection()) JsNull
    else	{
	    val value = columnType.asJavaClass() match {	      
	      case CassandraAscii => JsString(obj.getString(index))	      
	      case CassandraBigInt => JsNumber(obj.getLong(index))
	      case CassandraBlob => JsString("BLOB!") //TODO
	      case CassandraBoolean => JsBoolean(obj.getBool(index))
	      case CassandraCounter => JsNumber(obj.getLong(index)) 
	      case CassandraDecimal => JsNumber(obj.getDecimal(index))
	      case CassandraDouble => JsNumber(obj.getDouble(index))
	      case CassandraFloat => JsNumber(obj.getFloat(index))
	      case CassandraInet => JsString(obj.getInet(index).toString)
	      case CassandraInt => JsNumber(obj.getInt(index))
	      case CassandraList =>
	        val c = columnType.getTypeArguments.get(0).asJavaClass()
	        if (c != CassandraTuple) {
	          val list = obj.getList(index, c).map(_.toString)
	          list.toList.toJson
	        } else {
	        	val list = obj.getList(index, c).map(x => convertTuple(x.asInstanceOf[TupleValue],
	        	                                                     x.asInstanceOf[TupleValue].getType().getComponentTypes().toList))
	        	list.toList.toJson
	        }
	      case CassandraMap =>
	        val c = columnType.getTypeArguments.get(1).asJavaClass()
	        if (c != CassandraTuple) {
	          val map = obj.getMap(index, columnType.getTypeArguments.get(0).asJavaClass(),
	            c).map(x => (x._1.toString -> x._2.toString))
	          map.toMap.toJson
	        } else {
	
	          val map = obj.getMap(index, columnType.getTypeArguments.get(0).asJavaClass(),
	            c).map(x => {
	              (x._1.toString -> convertTuple(x._2.asInstanceOf[TupleValue],
	                x._2.asInstanceOf[TupleValue].getType().getComponentTypes().toList))
	            })
	          map.toMap.toJson
	        }
	      case CassandraSet =>
	        val c = columnType.getTypeArguments.get(0).asJavaClass()
	        if (c != CassandraTuple) {
	          val set = obj.getSet(index, c).map(_.toString)
	          set.toSet.toJson
	        } else {
	        	val set = obj.getSet(index, c).map(x => convertTuple(x.asInstanceOf[TupleValue],
	        	                                                     x.asInstanceOf[TupleValue].getType().getComponentTypes().toList))
	        	set.toSet.toJson
	        }
	      case CassandraText => JsString(obj.getString(index))
	      case CassandraTimestamp => JsNumber(obj.getDate(index).getTime())      
	      case CassandraTimeUUID => JsString(obj.getUUID(index).toString)    
	      case CassandraUUID => JsString(obj.getUUID(index).toString)
	      case CassandraVarChar => JsString(obj.getString(index))
	      case CassandraVarInt => JsNumber(obj.getVarint(index))
	      case CassandraTuple =>
	        val tupleValue = obj.getTupleValue(index)
	        convertTuple(tupleValue, tupleValue.getType().getComponentTypes().toList)
      }
    value
    }
  }
 
  def convertTuple(obj : TupleValue, components : List[DataType]) : JsValue = {
    var index = 0;
    val res = for (component <- components) yield {
    		index += 1
    		convertToJsValue(component, obj, index - 1)
    }
    res.toJson
  }

  def convertRow(row : Row) = {
    
    var result : Map[String, JsValue] = Map[String, JsValue]()
    var index = 0;
    val it = row.getColumnDefinitions().iterator()

    while (it.hasNext()) {
      val colDef = it.next()
      val columnType = colDef.getType()
      result += (colDef.getName() -> convertToJsValue(columnType, row, index))
      index += 1
    }
    result.toJson
  }
  
  def convertResultSet(rs : ResultSet) = {
    var res : List[JsValue] = Nil
    val it = rs.iterator()
    while(it.hasNext){
    	val row = it.next()
      res = res :+ convertRow(row)
    }
    res.toJson
  }
}

object CassandraJsonProtocol extends DefaultJsonProtocol {
  
  implicit object ResultSetFormat extends RootJsonWriter[ResultSet] {
    import CassandraTypes.convertResultSet
    def write(rs : ResultSet) = convertResultSet(rs)
  }

  implicit object RowFormat extends RootJsonWriter[Row] {
    import CassandraTypes.convertRow
    def write(row : Row) = convertRow(row)
  }

}
