<script setup lang="ts">
import { reactive, Ref, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";

const orcFileForm = reactive({ inputFiles: "" });


async function read_orc_file(filename:string) {
  // Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
  const  result :any =await invoke("read_orc_file", { filename: filename});
  console.log(result);


  columns.value = result.columns;
  data.value = result.rows;
}
const updateInputFiles = async () => {
  await read_orc_file(orcFileForm.inputFiles)
};

const data = ref([]);

interface Column {
  data_type: string
  name: string
}
const columns :Ref<Array<Column>> = ref([]);
</script>

<template>
  <el-form :model="orcFileForm" label-width="90px" size="small" @submit.native.prevent>
    <el-form-item label="inputFiles">
      <el-input
        v-model="orcFileForm.inputFiles"
        style="width: 100%"
        clearable
        @change="updateInputFiles"
      />
    </el-form-item>
  </el-form>

  <br />
  <el-table :data="data" min-height="240" border fit>
    <el-table-column label="__rowindex" width="150px" type="index" />
    <el-table-column v-for="column in columns" :key="column.name" :prop="column.name" :label="column.name" min-width="100px"/>
  </el-table>
</template>

<style scoped></style>
