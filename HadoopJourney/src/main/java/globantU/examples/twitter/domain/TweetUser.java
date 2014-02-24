package globantU.examples.twitter.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;

public class TweetUser implements Writable {

  private static final String LANG = "lang";
  private static final String SCREEN_NAME = "screen_name";
  private static final String NO_VALUE = "";

  private Text lang = new Text();
  private Text screenName = new Text();

  public void loadUser(JSONObject json) throws JSONException {
    String lang = json.isNull(LANG) ? NO_VALUE : json.getString(LANG);
    String screenName = json.isNull(SCREEN_NAME) ? NO_VALUE : json.getString(SCREEN_NAME);
    this.lang.set(lang);
    this.screenName.set(screenName);
  }

  public void write(DataOutput out) throws IOException {
    this.lang.write(out);
    this.screenName.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    this.lang.readFields(in);
    this.screenName.readFields(in);
  }

  public String getLang() {
    return this.lang.toString();
  }

  public String getScreenName() {
    return this.screenName.toString();
  }
}
