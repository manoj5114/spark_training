/**
 * Created by snudurupati on 5/5/15.
 * A basic introduction to Functional Programming in Scala.
 */

object ScalaPrimer {
  def main(args: Array[String]): Unit = {

    // 1. Anonymous functions or closures or lambda functions
    val lines = List("My first Scala program", "My first mysql query")

    //regular function
    def containsString(x: String) = x.contains("mysql")

    //higher order function
    lines.filter(containsString)

    //anonymous function
    lines.filter(s => s.contains("mysql"))

    //shortcut notation for anonymous functions
    lines.filter(_.contains("mysql"))

    //2. Type Inference
    def squareFunc(x: Int) = {      //function return type is inferred based on return value.
      x*x
    }

    val nums = List(1, 2, 3, 4, 5)
    val numSquares = nums.map(squareFunc) //functions map input values to output and do not change data in place
    println(numSquares)

    //3. Implicit Conversions
    val a: Int = 1; val b: Int = 4
    val myRange: Range = a to b

    myRange.foreach(println)      //here myRange was defined to be of type Range
    (1 to 4).foreach(println)     //here type Int is converted to type Range implicitly

    //4. Pattern Matching
    val pairs = List((1, 2), (2, 3), (3, 4))
    //no pattern matching
    pairs.filter(s => s._2 != 2)

    //case performs pattern matching over tuple
    pairs.filter{ case(x, y) => y != 2 }

    //5. Higher-order functions
    //filter, map, flatMap, reduce etc. are higher-order functions as they accept other functions as parameters.


  }


}
