<template>
  <div>
    <el-form :model="taskConfig" label-position="top">
      <!-- 在这里根据你的实际需求添加表单项 -->
      <el-form-item label="Task Type">
        <el-input v-model="taskConfig.taskType" disabled />
      </el-form-item>
      <el-form-item label="Task Parameters">
        <!-- 在这里添加表单项，根据你的实际参数结构 -->
        <!-- 例如： -->
        <el-input v-model="taskConfig.taskParam.entryCurrencyId" placeholder="Entry Currency ID" />
        <el-input v-model="taskConfig.taskParam.entryRatio" placeholder="Entry Ratio" />
        <!-- 其他参数依次类推 -->
      </el-form-item>
      <el-form-item label="Task Status">
        <el-input v-model="taskConfig.taskStatus" disabled />
      </el-form-item>
      <el-form-item label="Message">
        <el-input v-model="taskConfig.message" />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="createTask">Create Task</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
import axios from 'axios';
export default {
  data() {
    return {
      taskConfig: {
        taskType: 'IRRIGATE',
        taskParam: {
          entryCurrencyId: 0,
          entryRatio: 0,
          repetition: 0,
          containsVoucher: false,
          // 其他参数依次类推
        },
        taskStatus: 'READY',
        message: ''
      }
    };
  },
  methods: {
    createTask() {
      // 调用接口创建任务
      axios.post('http://localhost:8088/api/tasks/irrigate', this.taskConfig)
        .then(response => {
          console.log('Task created successfully:', response.data);
          // 清空表单或进行其他操作
        })
        .catch(error => {
          console.error('Error creating task:', error);
        });
    }
  }
};
</script>
