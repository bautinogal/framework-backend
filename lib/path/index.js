import path from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);

export const root =  path.join(path.dirname(__filename),'..','..');
export default { ...path, root };