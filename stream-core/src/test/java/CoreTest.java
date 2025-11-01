import com.stream.core.ConfigUtils;

/**
 * @Package PACKAGE_NAME.CoreTest
 * @Author zhou.han
 * @Date 2025/10/24 16:47
 * @description:
 */
public class CoreTest {

    public static void main(String[] args) {
        System.err.println(ConfigUtils.getString("mysql.host"));
    }

}
