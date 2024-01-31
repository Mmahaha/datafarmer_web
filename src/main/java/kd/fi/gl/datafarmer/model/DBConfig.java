package kd.fi.gl.datafarmer.model;

import lombok.Data;

/**
 * Description: db config pojo
 *
 * @author ysj
 * @date 2024/1/15
 */

@Data
public class DBConfig {

    private String host;
    private String port;
    private String user;
    private String password;
    private String fiDatabase;
    private String sysDatabase;
}
