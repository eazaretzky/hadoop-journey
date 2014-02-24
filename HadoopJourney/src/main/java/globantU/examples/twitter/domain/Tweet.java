package globantU.examples.twitter.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;

public class Tweet implements Writable {

  private static final String TEXT = "text";
  private static final String USER = "user";

  private final Text text = new Text();
  private final TweetUser user = new TweetUser();

  public void loadTweet(final String json) throws JSONException {
    JSONObject tweet = new JSONObject(json);
    Validate.isTrue(tweet.has(TEXT) && !tweet.isNull(TEXT));
    Validate.isTrue(tweet.has(USER) && !tweet.isNull(USER));

    this.text.set(tweet.getString(TEXT));
    this.user.loadUser(tweet.getJSONObject(USER));
  }

  public void readFields(DataInput in) throws IOException {
    this.text.readFields(in);
    this.user.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    this.text.write(out);
    this.user.write(out);
  }

  public String getText() {
    return this.text.toString();
  }

  public String getLang() {
    return this.user.getLang();
  }

  public String getScreenName() {
    return this.user.getScreenName();
  }
}
