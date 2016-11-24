import com.twitter.finagle.Service
import com.twitter.finagle.http
import com.twitter.finagle.Http
import com.twitter.finagle.Redis
import com.twitter.util.{Future, Await}
import com.twitter.io.Buf
import com.twitter.io.Bufs
import java.net.URLEncoder;
import play.api.libs.json.JsValue
import play.api.libs.json.Json

case class Post(content: String, user: User)
case class User(name: String, age: String)

object Timeline {
  val usersClient = Http.client.withAdmissionControl.maxPendingRequests(1000).newService("104.131.176.96:3000")
  //val usersClient = Http.client.withAdmissionControl.maxPendingRequests(1000).newService("localhost:3000")
  val redisClient = Redis.client.withAdmissionControl.maxPendingRequests(1000).newRichClient("localhost:6379")

  def main(args: Array[String]): Unit = {
    val server = Http.server.serve(":8080", service)
    Await.ready(server)
  }
  
  val service = new Service[http.Request, http.Response] {
    def apply(req: http.Request): Future[http.Response] = {
      val key = Buf.Utf8("timeline:" + req.getParam("id"))
      redisClient.lRange(key, 0, -1) flatMap buildResponse
    }

    def buildResponse(l: List[Buf]) = {
      val post_contents = redisClient.mGet(l.map(Buf.Utf8("post_content:").concat(_)))
      val post_users = redisClient.mGet(l.map(Buf.Utf8("post_user:").concat(_)))
      
      val users = post_users flatMap { user_ids =>
        val usersToLoad = user_ids.map(id => Bufs.asUtf8String(id.getOrElse(Buf.Utf8("")))).mkString("{\"ids\": [", ",", "]}")
        val request = http.Request(http.Method.Get, "?ids=" + URLEncoder.encode(usersToLoad, "UTF-8"))
        usersClient(request)
      }

      Future.join(post_contents, users) flatMap { case(p, u) =>
        // json serialization
        implicit val userFormat = Json.format[User]
        implicit val postFormat = Json.format[Post]
        val users:Seq[User] = Json.parse(u.getContentString()).as[Seq[User]]
        val json = Json.toJson((p, users).zipped.map((content, user) => {
          new Post(Bufs.asUtf8String(content.getOrElse(Buf.Utf8(""))), user)
        }))
        val res = http.Response(http.Versions.HTTP_1_1, http.Status.Ok)
        res.setContentString(json.toString)
        Future.value(res)
      }
    }

  }
}
