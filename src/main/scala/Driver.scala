import scala.sys.process._
import scala.util.Random

object Driver extends App {
  "rm -rf ./log-files" !
  val bitcask: DB = new Bitcask
  (1 to 10000).foreach(e => bitcask.put(e.toString, e.toString))
  (5000 to 10000).foreach(e => bitcask.put(e.toString, (-e).toString))
  val addAndUpdate = (1 to 1000).map(_ => Random.nextInt(10000))
    .map(_.toString)
    .map(bitcask.get)
    .filter(_.isDefined)
    .flatten
    .map(_.toInt)
    .forall(x => (Math.abs(x) < 5000 && x < 5000) || (Math.abs(x) > 5000 && x < 0))
  if (!addAndUpdate) throw new RuntimeException("Wrong Update")

  (200 to 4000).foreach(x => bitcask.remove(x.toString))
  val removal = (200 to 4000).forall(x => bitcask.get(x.toString).isEmpty)
  if (!removal) throw new RuntimeException("Removal doesn't work")

}
