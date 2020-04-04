val myList = List("abc", "def", "ghi")

val mapped = myList.map(s => s.toUpperCase)

/* scala basics*/

// variables
val text : String = "U cant modify me"

var changeMe : String = "U can update me"
changeMe = "I am modified"

// Function declaration
def sayHello(name : String) : String = {
  s"Hello $name!"
}

// multi parameter list
def sayHello(name : String) (myself : String)= {
  s"Hello $name! My name is $myself"
}

// Function parameter list
def sayHello2(name : String) (whoAreYou : () => String)= {
  s"Hello $name! My name is ${whoAreYou()}"
}
def provideName() = {"Scala"}
//val fast = sayHello2("test")(provideName())
val faster = sayHello2("test") { () => "Anonymous"}

// Implicit
def sayHello3(name : String) (implicit myself : String)= {
  s"Hello $name! My name is $myself"
}
implicit val myString = "implicits"
val fast = sayHello3("test") // myString implicityl gets added here

def sayHello4(name : String) (implicit whoAreYou : () => String)= {
  s"Hello $name! My name is ${whoAreYou()}"
}
/*not working hence commented --
implicit def provideName2() = { "Scala" }
val fast2 = sayHello4("test")*/

// Class definition
class fastTrack( name: String, myself: String) {
  def sayHello5(name: String)(myself: String)= {
    s"Hello $name ! My name is $myself"
  }

  //val greeting = sayHello5(name, myself)
}

// case class
case class person(fname: String, lname: String)
val me = person("Dhaval", "Shah")
println(me.fname)

// Pattern matching with case classes
abstract class Person(fname: String, lname: String) {
  def fullName = {s"$fname-$lname"}
}

case class Student(fname: String, lname: String, id: Int) extends Person(fname, lname)

val me2 = Student("Dhaval", "Shah", 10)

def getFullID[T <: Person](something: T) = {
  something match {
    case Student(fname, lname, id) => s"$fname-$lname-$id"
    case p: Person => p.fullName
  }
}
getFullID(me2)

// Implicit Conversions
implicit class stringUtils(myString: String) {
  def scalaWordCount() = {
    val split = myString.split("\\s+")
    val grouped = split.groupBy(word => word)
    val countPerKey = grouped.mapValues(group => group.length)
    countPerKey
  }
}
"Spark collections mimic Scala collections".scalaWordCount()

/** Scala Collections*/
val myList2 = List("Spark", "mimics", "Scala", "Collections")

// map
val mapped2 = myList2.map(s => s.toUpperCase)

//flatMap - it unboxes the return result
val flatMapped = myList2.flatMap { s2 =>
  val filters = List("mimcs", "collections")
  if (filters.contains(s2))
    None
  else
    Some(s2)
}
