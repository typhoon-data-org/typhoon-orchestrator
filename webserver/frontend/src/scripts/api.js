import axios from "axios";

const API = axios.create({baseURL: 'http://localhost:5000/'});

export default {
  getDagFilenames: () => API.get('get-dag-filenames'),
  getDAGContents: params => API.get('get-dag-contents', {params}),
  saveDAGCode: (body, params) => API.put('save-dag-code', body, {params}),
}
