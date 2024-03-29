package kd.fi.gl.datafarmer.controller;

import kd.fi.gl.datafarmer.common.ApiResponse;
import kd.fi.gl.datafarmer.common.db.JdbcTemplateContainer;
import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.helper.DDLSqlHelper;
import kd.fi.gl.datafarmer.model.DBConfig;
import lombok.RequiredArgsConstructor;
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
    private final DB db;

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

    @GetMapping("/debug")
    public ApiResponse<Boolean> debug() {
//        DDLSqlHelper ddlSqlHelper = DB.getDDLSqlHelper();
//        for (Integer index : container.getFi().queryForList("select findex from t_gl_voucher$map", Integer.class)) {
//            for (String queryIndex : ddlSqlHelper.queryIndexes("t_gl_voucher$" + index)) {
//                if (queryIndex.contains("_pkey")) {
//                    ddlSqlHelper.dropConstraint("t_gl_voucher$" + index, queryIndex);
//                } else {
//                    ddlSqlHelper.dropIndex(queryIndex);
//                }
//            }
//
//            for (String queryIndex : ddlSqlHelper.queryIndexes("t_gl_voucherentry$" + index)) {
//                if (queryIndex.contains("_pkey")) {
//                    ddlSqlHelper.dropConstraint("t_gl_voucherentry$" + index, queryIndex);
//                } else {
//                    ddlSqlHelper.dropIndex(queryIndex);
//                }
//            }
//        }
        return ApiResponse.success(Boolean.TRUE);
    }
}
