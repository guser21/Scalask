import scala.util.Random

object Driver extends App {
  //  "rm -rf ./log-files" !
  val bitcask: DB = new Bitcask


  (1 to 10000).foreach(e => bitcask.put(e.toString, e.toString))
  (5000 to 10000).foreach(e => bitcask.put(e.toString, (-e).toString))


  bitcask.get("400")
  val wrongUpdate = (1 to 3000).map(_ => Random.nextInt(10000))
    .map(_.toString)
    .map(bitcask.get)
    .filter(_.isDefined)
    .flatten
    .map(_.toInt)
    .find(x => !((Math.abs(x) < 5000 && x < 5000 && x >= 0) || (Math.abs(x) >= 5000 && x < 0)))
  if (wrongUpdate.isDefined) throw new RuntimeException("wrong update" + wrongUpdate.get)


  (200 to 4000).foreach(x => bitcask.remove(x.toString))

  val removal = (200 to 4000).forall(x => bitcask.get(x.toString).isEmpty)
  if (!removal) throw new RuntimeException("Removal doesn't work")

}
