package kd.fi.gl.datafarmer.core.config;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class DBConfigVO {

    private String fiConURL;

    private String sysConURL;

    private String username;

    private String password;

    public static DBConfigVO loadFromMap(Map<String, Object> props) {
        DBConfigVO configVO =  new DBConfigVO();
        String dbType = StringUtils.isEmpty((String)props.get("dbtype")) ? "postgresql" : (String)props.get("dbtype");
        String host = (String) _checkValue(props, "host", true);
        int port = (Integer) _checkValue(props, "port", true);
        configVO.username = (String) _checkValue(props, "user", true);
        configVO.password = (String) _checkValue(props, "passwd", true);
        configVO.fiConURL = String.format("jdbc:%s://%s:%s/%s?defaultRowFetchSize=5000&useCursorFetch=true", dbType, host, port, (String)props.get("fiScheme"));
        configVO.sysConURL = String.format("jdbc:%s://%s:%s/%s?defaultRowFetchSize=5000&useCursorFetch=true", dbType, host, port, (String)props.get("sysSchema"));

        return configVO;
    }

    private static Object _checkValue(Map<String, Object> props, String key, boolean isRequired) {
        Object v = props.get(key);
        if (isRequired && null == v) {
            throw new RuntimeException("config item:" + key + " is required, must specify.");
        }
        return v;
    }

    public String getFiConURL() {
        return fiConURL;
    }

    public void setFiConURL(String fiConURL) {
        this.fiConURL = fiConURL;
    }

    public String getSysConURL() {
        return sysConURL;
    }

    public void setSysConURL(String sysConURL) {
        this.sysConURL = sysConURL;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "DBConfig{" +
                "fiConURL='" + fiConURL + '\'' +
                ", sysConURL='" + sysConURL + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
