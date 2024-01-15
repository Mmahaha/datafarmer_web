<template>
  <div class="app-container">

    <el-form ref="form" :model="config" label-width="120px">
      <el-form-item label="主机ip">
        <el-input v-model="config.host" style="width: 300px"/>
      </el-form-item>
      <el-form-item label="端口">
        <el-input v-model="config.port"/>
      </el-form-item>
      <el-form-item label="用户名">
        <el-input v-model="config.user"/>
      </el-form-item>
      <el-form-item label="密码">
        <el-input v-model="config.password" type="password"/>
      </el-form-item>
      <el-form-item label="财务库名">
        <el-input v-model="config.fiDatabase"/>
      </el-form-item>
      <el-form-item label="系统库名">
        <el-input v-model="config.sysDatabase"/>
      </el-form-item>
      <el-form-item style="display: flex; align-items: center;">
        <div v-if="initialized" style="color: green; margin-right: 10px;">当前已初始化</div>
        <div v-else style="color: red; margin-right: 10px;">当前未初始化</div>
        <el-button type="primary" @click="onSubmit">初始化</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import axios from 'axios';

export default {
  data() {
    return {
      config: {
        host: '',
        port: '',
        user: '',
        password: '',
        fiDatabase: '',
        sysDatabase: '',
      },
      initialized: false, // 初始化状态
    };
  },
  created() {
    this.fetchCurrentConfig();
    this.fetchInitializationStatus(); // 调用接口获取初始化状态
  },
  methods: {
    onSubmit() {
      axios.post('http://localhost:8088/api/dbconfigs/init', this.config)
        .then(response => {
          this.$message(` ${JSON.stringify(response.data)} `);
          this.fetchInitializationStatus();
        })
        .catch(error => {
          this.$message(` ${JSON.stringify(error)} `);
        });
    },
    fetchCurrentConfig() {
      axios.get('http://localhost:8088/api/dbconfigs/current')
        .then(response => {
          const { host, port, user, password, fiDatabase, sysDatabase } = response.data.data;
          this.config = { host, port, user, password, fiDatabase, sysDatabase };
        })
        .catch(error => {
          this.$message(`Failed to fetch current config: ${JSON.stringify(error)}`);
        });
    },
    // 新增方法，获取初始化状态
    fetchInitializationStatus() {
      axios.get('http://localhost:8088/api/dbconfigs/initialized')
        .then(response => {
          this.initialized = response.data.data;
        })
        .catch(error => {
          this.$message(`Failed to fetch initialization status: ${JSON.stringify(error)}`);
        });
    },
  },
};
</script>

<style scoped>
.line {
  text-align: center;
}

.app-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100vh;
}
</style>
