<script setup lang="ts">
import { reactive, Ref, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { open, save } from "@tauri-apps/plugin-dialog";
import { ElLoading, ElMessage } from "element-plus";
const orcFileForm = reactive({ inputFiles: "" });

/*
async function read_orc_file(filename:string) {
  // Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
  const  result :any =await invoke("read_orc_file", { filename: filename});
  console.log(result);


  columns.value = result.columns;
  data.value = result.rows;

  orc_struct.value= JSON.stringify(result.columns);

  total.value = result.total;
}*/

async function read_orc_file_by_page(
  filename: string,
  page_size: number,
  page_number: number
) {
  // Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
  const result: any = await invoke("read_orc_file_by_page", {
    fileName: filename,
    pageSize: page_size,
    pageNumber: page_number,
  });
  console.log(result);

  columns.value = result.columns;
  data.value = result.rows;

  orc_struct.value = JSON.stringify(result.columns);

  total.value = parseInt(result.total);
}

async function export_orc_file_csv(
  orc_filename: string,
  target_csv_file_path: string
) {
  // Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
  try {
    await invoke("export_orc_file_csv", {
      orcFilename: orc_filename,
      targetCsvFilePath: target_csv_file_path,
    });
    ElMessage({
      showClose: true,
      message: "export success",
      type: "success",
    });
  } catch (e: any) {
    console.log(e)
    ElMessage({
      showClose: true,
      message: e.toString(),
      type: "error",
    });
  }
}

const data = ref([]);

const openFile = async () => {
  const selected = await open({
    multiple: false,
    directory: false,
    filters: [
      {
        name: "ORC Files",
        extensions: ["orc"],
      },
    ],
  });
  if (selected) {
    orcFileForm.inputFiles = selected;
    await read_orc_file_by_page(
      orcFileForm.inputFiles,
      pageSize.value,
      currentPage.value
    );
  }
};

const exportCsv = async () => {
  if(!orcFileForm.inputFiles || orcFileForm.inputFiles==""){
    ElMessage({
      showClose: true,
      message: "please select a orc file first",
      type: "error",
    });
    return
  }
  const selected = await save({
    filters: [
      {
        name: "Csv Files",
        extensions: ["csv"],
      },
    ],
    defaultPath: orcFileForm.inputFiles.replace("\\","/").split("/").pop()+".csv",
  });
  if (selected) {
    const loadingInstance1 = ElLoading.service({ fullscreen: true });
    await export_orc_file_csv(
      orcFileForm.inputFiles,
      selected,
    );
    loadingInstance1.close();
  }
};

interface Column {
  data_type: string;
  name: string;
}
const columns: Ref<Array<Column>> = ref([]);

const strunctdrawer = ref(false);

const orc_struct = ref("");

const openStruct = async () => {
  if(!orcFileForm.inputFiles || orcFileForm.inputFiles==""){
    ElMessage({
      showClose: true,
      message: "please select a orc file first",
      type: "error",
    });
    return
  }
  strunctdrawer.value = !strunctdrawer.value;
};

const pageSize = ref(10);
const total = ref(0);
const currentPage = ref(1);
const handleCurrentChange = async (val: number) => {
  currentPage.value = val;

  await read_orc_file_by_page(
    orcFileForm.inputFiles,
    pageSize.value,
    currentPage.value
  );
};
const handleSizeChange = async (val: number) => {
  pageSize.value = val;
  await read_orc_file_by_page(
    orcFileForm.inputFiles,
    pageSize.value,
    currentPage.value
  );
};
</script>

<template>
  <el-form
    :model="orcFileForm"
    label-width="150px"
    size="small"
    @submit.native.prevent
  >
    <el-form-item label="Input File Path:">
      <input style="width: 300px" clearable v-model="orcFileForm.inputFiles" />
      <el-button @click="openFile">Open</el-button>
      <el-button @click="exportCsv">Export Csv</el-button>
      
      <el-button @click="openStruct">show struct</el-button>
    </el-form-item>
  </el-form>

  <br />
  <el-table :data="data" min-height="240" border fit>
    <el-table-column label="__rowindex" width="150px">
      <template #default="scope">
        {{ scope.$index + 1 + pageSize * (currentPage - 1) }}
      </template>
    </el-table-column>
    <el-table-column
      v-for="column in columns"
      :key="column.name"
      :prop="column.name"
      :label="column.name"
      min-width="100px"
    >
      <template #header>
        <span :title="column.data_type"> {{ column.name }}</span>
      </template>
    </el-table-column>
  </el-table>
  <el-pagination
    @current-change="handleCurrentChange"
    :current-page="currentPage"
    :page-size="pageSize"
    :total="total"
    :page-sizes="[10, 100, 200, 300, 400]"
    layout="prev, pager, next, sizes, jumper,total"
    @size-change="handleSizeChange"
  ></el-pagination>
  <el-drawer v-model="strunctdrawer" title="Struct">
    {{ orc_struct }}
  </el-drawer>
</template>

<style scoped></style>
