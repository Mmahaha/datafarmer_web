package kd.fi.gl.datafarmer.controller;

import kd.fi.gl.datafarmer.common.ApiResponse;
import kd.fi.gl.datafarmer.common.db.JdbcTemplateContainer;
import kd.fi.gl.datafarmer.model.DBConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/15
 */

@RestController()
@RequestMapping("/api/dbconfigs")
@CrossOrigin(originPatterns = "*")
@RequiredArgsConstructor
public class DBConfigController {

    private final JdbcTemplateContainer container;

    @PostMapping("/init")
    public ApiResponse<String> createDBConfig(@RequestBody DBConfig dbConfig) {
        container.init(dbConfig);
        return ApiResponse.success("init success");
    }

    @GetMapping("/initialized")
    public ApiResponse<Boolean> isDatabaseInitialized() {
        return ApiResponse.success(container.isInitialized());
    }

    @GetMapping("/current")
    public ApiResponse<DBConfig> getCurrentDBConfig() {
        return ApiResponse.success(container.getDbConfig());
    }
}
