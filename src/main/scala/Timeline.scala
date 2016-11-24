import com.twitter.finagle.Service
import com.twitter.finagle.http
import com.twitter.finagle.Http
import com.twitter.finagle.Redis
import com.twitter.util.{Future, Await}
import com.twitter.io.Buf
import com.twitter.io.Bufs
import play.api.libs.json.JsValue
import play.api.libs.json.Json

case class Post(content: String, user: String)

object Timeline {
  val usersClient = Http.client.withAdmissionControl.maxPendingRequests(1000).newService("104.131.176.96:3000")
  val redisClient = Redis.client.withAdmissionControl.maxPendingRequests(1000).newRichClient("localhost:6379")

  def main(args: Array[String]): Unit = {
    val server = Http.server.serve(":3000", service)
    Await.ready(server)
  }

  val service = new Service[http.Request, http.Response] {
    def apply(req: http.Request): Future[http.Response] = {
      val key = Buf.Utf8("timeline:" + req.getParam("id"))
      redisClient.lRange(key, 0, -1) flatMap getPosts
    }

    def getPosts(l: List[Buf]) = {
      val post_contents = redisClient.mGet(l.map(Buf.Utf8("post_content:").concat(_)))
      val post_users = redisClient.mGet(l.map(Buf.Utf8("post_user:").concat(_)))
      
      val f = Future.collect(Seq(post_contents, post_users)) flatMap getContents
      //val res = http.Response(http.Versions.HTTP_1_1, http.Status.Ok);
      //res.setContentTypeJson()
      // res.setContentString();
      // Future.value(res)
    }
    
    def getContents(l: Seq[Seq[Option[Buf]]]) = {
      val posts:Seq[Post] = (l.head, l.tail.head).zipped.map((content, user_id) => {
        val id = Bufs.asUtf8String(user_id.getOrElse(Buf.Utf8("")))
        new Post(Bufs.asUtf8String(name.getOrElse(Buf.Utf8(""))), Bufs.asUtf8String(age.getOrElse(Buf.Utf8(""))))
      })
      l.map(c => Bufs.asUtf8String(c.getOrElse(Buf.Utf8(""))))
    }

    def getUsers(l: Seq[String]) = {
      val j = "{\"users\": [ {\"name\": \"John\", age: 32}, {\"name\": \"Carl\", age: 30}, {\"name\": \"Gustave\", age: 22} ]"
      val json: JsValue = Json.parse(j)
      println(json)
      j
    }
  }
}
