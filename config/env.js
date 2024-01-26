// Levanta las variables de entorno del archivo .env
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.join(__dirname, '.env') })

//------------------------------- Enviroment Variables ----------------------------------
export default {
  app: {
    env: process.env.APP_NODE_ENV || 'development',
    pwd: process.env.APP_PWD || "",
    ssl_port: parseInt(process.env.APP_SSL_PORT) || 443,
    port: parseInt(process.env.APP_PORT) || 8080,
    frontEndUrl: process.env.FRONT_END_URL,
    logLevel: parseInt(process.env.APP_LOG_LEVEL) || 0
  },
  jwt: {
    secret: process.env.JWT_SECRET || 'secret',
    saltWorkFactor: process.env.JWT_SWF || '10'
  },
  tables:{
    pg: {
      host: process.env.PG_HOST || 'localhost',
      port: process.env.PG_PORT || 5432,
      database: process.env.PG_DDBB || 'demo',
      user: process.env.PG_USER || 'postgres',
      password: process.env.PG_PASSWORD || 'postgres',
    },
    cdc:{
      prefix: process.env.TABLES_CDC_PREFIX || 'cdc_', 
      id: process.env.TABLES_CDC_ID || 'cdc_id', 
      action: process.env.TABLES_CDC_ACTION || 'cdc_action', 
      createdAt: process.env.TABLES_CDC_CREATED_AT || 'cdc_created_at'
    }
  }

}