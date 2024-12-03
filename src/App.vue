<script setup lang="ts">
import { reactive, Ref, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { open } from '@tauri-apps/plugin-dialog';

const orcFileForm = reactive({ inputFiles: "" });


async function read_orc_file(filename:string) {
  // Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
  const  result :any =await invoke("read_orc_file", { filename: filename});
  console.log(result);


  columns.value = result.columns;
  data.value = result.rows;
}

const data = ref([]);

const openFile = async () => {
  const selected = await open({
    multiple: false,
    directory: false,
    filters: [{
      name: 'ORC Files',
      extensions: ['orc']
    }]
  })
  if (selected) {
    orcFileForm.inputFiles = selected;
    await read_orc_file(orcFileForm.inputFiles)
  }
}

interface Column {
  data_type: string
  name: string
}
const columns :Ref<Array<Column>> = ref([]);
</script>

<template>
  <el-form :model="orcFileForm" label-width="150px" size="small" @submit.native.prevent>
    <el-form-item label="Input File Path:">
      <input  
        style="width: 90%"
        clearable
        v-model="orcFileForm.inputFiles"
      />
      <el-button @click="openFile">Open</el-button>
    </el-form-item>
  </el-form>

  <br />
  <el-table :data="data" min-height="240" border fit>
    <el-table-column label="__rowindex" width="150px" type="index" />
    <el-table-column v-for="column in columns" :key="column.name" :prop="column.name" :label="column.name" min-width="100px"/>
  </el-table>
</template>

<style scoped></style>
