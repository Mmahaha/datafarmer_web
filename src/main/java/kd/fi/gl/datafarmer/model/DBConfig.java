package kd.fi.gl.datafarmer.model;

/**
 * Description: db config pojo
 *
 * @author ysj
 * @date 2024/1/15
 */

public class DBConfig {

    private String host;
    private String port;
    private String user;
    private String password;
    private String fiDatabase;
    private String sysDatabase;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getFiDatabase() {
        return fiDatabase;
    }

    public void setFiDatabase(String fiDatabase) {
        this.fiDatabase = fiDatabase;
    }

    public String getSysDatabase() {
        return sysDatabase;
    }

    public void setSysDatabase(String sysDatabase) {
        this.sysDatabase = sysDatabase;
    }
}
