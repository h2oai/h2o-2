package water.api.dsl

import scala.collection.immutable.Range
import water.fvec.Frame
import water.Key

/** The object carry global environment and provides basic global methods such as head, tail, nrows, ... */
object H2ODsl extends H2ODslImplicitConv with T_R_Env[DFrame] with T_H2O_Env[HexKey, DFrame] with DefaultEnv[HexKey, DFrame] {
  // Binary operator type alias
  type BOp = T_NV_Transf[scala.Double]
  // Filter operator type alias
  type FOp = T_NF_Transf[scala.Double]
  // Array operator type alias
  type AOp = T_A2A_Transf[scala.Double,scala.Double]
  // Array filter type alias
  type FAOp = T_A2B_Transf[scala.Double]
  // Double collector
  type CDOp = T_T_Collect[scala.Double]
    
  // An object to represent an empty range selector 
  case object * extends Range(0, -1, 1);
  
  def frame(kname:String):DFrame = new DFrame(get(kname))
  def echo = println
  def load(k:HexKey) = new DFrame(get(k))
  def save(k:HexKey, d:DFrame) = put(k,d)
  
  def example():DFrame = example(System.getProperty("user.dir") + "/../../")

  def example(topdir:String):DFrame = {
    println("topdir is: " + topdir)
    val tdir = if (topdir==null) "" else if (!topdir.endsWith("/")) topdir+"/" else topdir
    println("Looking for smalldata directory in " + tdir)
    println("""
== Parsing smalldata/cars.csv""")
    val f = parse(tdir+"smalldata/cars.csv", "cars.hex")
    val g = load("cars.hex")
    val g1 = g ++ (g("year") > 80)
    save("cars.hex", g1)
    val f1 = f("year") + 1900 
    println("""
== Number of cylinders - f("cylinders")""")
    println(f("cylinders"))
    println("""
== Selecting a set of columns and storing a frame reference - val f2 = f(2::3::7::Nil)""")
    val f2 = f(2::3::7::Nil)
    println(f2)
    println("""
== Using boolean transformation on the last column - f("year") > 80 """)
    val f3 = f("year") > 80
    println(f3)
    println("""
== Launching map function on a select column:
    f("cylinders") map (new BOp { 
      var sum:scala.Double = 0
      def apply(rhs:scala.Double) = { sum += rhs; rhs*rhs / sum; } 
    })""")
    val f4 = f("cylinders") map (new BOp { 
      var sum:scala.Double = 0
      def apply(rhs:scala.Double) = { sum += rhs; rhs*rhs / sum; } 
    })  
    println(f4)
    
    val f5 = f map ( new FAOp {
      def apply(rhs: Row):Boolean = rhs.d(2) > 4;
    });
    f
  }
  
  def pwd() = println(System.getProperty("user.dir"))
  
  def demo() = {
    println("""
=== DEMO ===
val f = parse("private/cars.csv")
f(*,0)
f(0)
f(0::2::7::Nill)
f("year")
f("year")+1900
val f4 = f map ( new AOp {
      def apply(rhs: Array[scala.Double]):Array[scala.Double] = { rhs(2) = -1; rhs; }
    });
        
f("cylinders") > 4
val f5 = f map ( new FAOp {
      def apply(rhs: Array[scala.Double]):Boolean = rhs(2) > 4;
    });
nrows(f5)
        
val f6 = f filter ( new FAOp {
      def apply(rhs: Array[scala.Double]):Boolean = rhs(2) > 4;
    });
nrows(f5)

val f7 = f collect ( 0.0, new CDOp() {
      override def apply(acc:scala.Double, rhs:Array[scala.Double]) = acc + rhs(2)
      override def reduce(l:scala.Double,r:scala.Double) = l+r
} )

class Avg(var sum:scala.Double, var cnt:Int) extends Iced;
val f8 = f collect ( new Avg(0,0), 
  new T_T_Collect[Avg,scala.Double] {
      override def apply(acc:Avg, rhs:Array[scala.Double]):Avg = {
        acc.sum += rhs(2)
        acc.cnt += 1
        return acc
      }
      override def reduce(l:Avg,r:Avg) = new Avg(l.sum+r.sum, l.cnt+r.cnt)
	} )       
        

val source = f(1) ++ f(3 to 7)
val response = f(2)
val model = drf(source, response, 1)
        
""")
  }

  override def head(d:DFrame, rows:Int) = println(d.toStringHead(rows))
  override def tail(d:DFrame, rows:Int) = println(d.toStringTail(rows))
  
  def test():DFrame = {
    val f = parse("../smalldata/cars.csv")
    f("cylinders") map (new BOp { 
      var sum:scala.Double = 0
      def apply(rhs:scala.Double) = { sum += rhs; rhs*rhs / sum; } 
    })
  }
}

/** A wrapper for H2O Frame proving basic operations */
class DFrame(private val _frame:Frame = new Frame) extends T_Frame with T_MR[DFrame] {
  import water.api.dsl.Utils._;
  
  def frame() = _frame
  
  def ncol() = frame().numCols()
  def nrow() = frame().numRows()
  def names() = frame().names()
  
  def apply(cols: Seq[Int]) = new DFrame(ffilterByIdx(frame(), cols))
  def apply(cols: String) = new DFrame(ffilterByName(frame, cols::Nil))
  def apply(rows: Seq[Int], cols: Seq[Int]) = apply(cols)
  def apply(rows: Seq[Int], cols: String) = apply(cols)
  def apply(f: Frame) = new DFrame(f)
  
  def +(rhs: Number) = map(Add(rhs.doubleValue()))
  def -(rhs: Number) = map(Sub(rhs.doubleValue()))
  def *(rhs: Number) = map(Mul(rhs.doubleValue()))
  // another way how to deal with map is to call Java low-leve API directly
  def /(rhs: Number) = apply(MRUtils.div(frame(), rhs.doubleValue()))
  def ^(rhs: Number) = apply(MRUtils.pow(frame(), rhs.doubleValue()))

  def < (rhs: Number) = map(Greater(rhs.doubleValue()))
  def >=(rhs: Number) = map(LessOrEqual(rhs.doubleValue()))
  def > (rhs: Number) = map(Less(rhs.doubleValue()))
  def <=(rhs: Number) = map(GreaterOrEqual(rhs.doubleValue()))
  def ==(rhs: Number) = map(Equal(rhs.doubleValue()))
  def !=(rhs: Number) = map(NEqual(rhs.doubleValue()))
  
  // Operations between frames - always return a new frame holding new data
  def +(rhs: T_Frame) = Utils.combine(this, rhs, CAdd())
  def -(rhs: T_Frame) = Utils.combine(this, rhs, CSub())
  // Append
  def ++(rhs: T_Frame) = new DFrame(cbind(frame(), rhs.frame()))
  override def toString() = Utils.head(frame(), NHEAD)
  def toStringHead(nrows: Int) = Utils.head(frame(), nrows)
  def toStringTail(nrows: Int) = Utils.tail(frame(), nrows)
  
}

sealed case class HexKey(key: Key) extends TRef {
  def name = key.toString()
}


trait H2ODslImplicitConv {
  // Implicit conversion from Int to Range
  implicit def Int2Range(n: Int): Range = Range(n, n + 1, 1)
  implicit def String2HexKey(s: String): HexKey = HexKey(Key.make(s))
  //  implicit def String2DFrame(s: String): DFrame = DFrame(s)
  implicit def DFrame2Frame(d: DFrame):Frame = d.frame()
  implicit def Frame2DFrame(f: Frame):DFrame = new DFrame(f)
} 

