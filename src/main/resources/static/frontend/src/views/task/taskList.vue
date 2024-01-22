<template>
  <div>
    <el-table
      :data="taskList"
      style="width: 100%"
      border
      stripe
    >
      <el-table-column
        prop="id"
        label="ID"
        width="80"
      ></el-table-column>
      <el-table-column
        prop="taskType"
        label="Task Type"
      ></el-table-column>
      <el-table-column
        label="Task Parameters"
      >
        <template slot-scope="scope">
          <el-collapse :value="isCollapseExpanded">
            <el-collapse-item title="View Parameters">
              <!-- 在 el-form 上添加 label-width 属性 -->
              <el-form label-position="left" inline class="param-form" :label-width="labelWidth">
                <!-- 遍历 taskParam 的所有字段 -->
                <el-form-item v-for="(value, key) in scope.row.taskParam" :key="key" :label="key">
                  {{ value }}
                </el-form-item>
              </el-form>
            </el-collapse-item>
          </el-collapse>
        </template>
      </el-table-column>
      <el-table-column
        prop="taskStatus"
        label="Task Status"
      ></el-table-column>
      <el-table-column
        prop="message"
        label="Message"
      ></el-table-column>
    </el-table>
  </div>
</template>

<script>
import axios from 'axios';

export default {
  data() {
    return {
      taskList: [],
      isCollapseExpanded: ['View Parameters'],
      labelWidth: '180px' // 设置 label 宽度
    };
  },
  mounted() {
    // 调用接口获取任务列表
    this.fetchTaskList();
  },
  methods: {
    fetchTaskList() {
      axios.get('http://localhost:8088/api/tasks')
        .then(response => {
          this.taskList = response.data.data;
        })
        .catch(error => {
          console.error('Error fetching task list:', error);
        })
        .finally(() => {
          // 数据加载完成后，将 isCollapseExpanded 设置为 true
          this.isCollapseExpanded = ['View Parameters'];
        });
    }
  }
};
</script>

<style scoped>
.param-form {
  max-height: 500px;
  overflow-y: auto;
}
</style>
