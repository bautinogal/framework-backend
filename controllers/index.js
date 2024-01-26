import env from '../config/env.js';
import express from 'express';
import path from 'path';
import { log, utils as u } from '../lib/index.js'
import h from './helpers.js';
const router = express.Router();
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
import { z, ZodError } from 'zod';
import { schema, schemasHash, getCache, query, getRows, getRow, getPaginatedRows, getRowCDC, postRow, editRow, delRow, addListener, removeListener } from '../lib/tables/index.js';


router.get('/api/:tableName/rows', async (req, res, next) => {
  const start = Date.now();
  let r = req?.query?.q ? await getPaginatedRows(req.params.tableName, req.query.q) : await getRows(req.params.tableName);
  console.log(`/api/${req.params.tableName}/rows ${req.query.q} Time ${Date.now() - start}ms`)
  res.send(r);
});
router.get('/api/:tableName/row/:where', async (req, res, next) => {
  const start = Date.now();
  const r = await getRow(req.params.tableName, JSON.parse(req.params.where));
  console.log(`/api/${req.params.tableName}/row/${req.params.where} Time ${Date.now() - start}ms`)
  res.send({ r });
});
router.get('/api/:tableName/row/history/:where', async (req, res, next) => {
  const start = Date.now();
  const r = await getRowCDC(req.params.tableName, JSON.parse(req.params.where));
  console.log(`/api/${req.params.tableName}/rows/history/${req.params.where} Time ${Date.now() - start}ms`)
  res.send(r);
});
router.get('/api/:tableName', async (req, res, next) => {
  const start = Date.now();
  const r = (await getCache())[req.params.tableName];
  console.log(`/api/${req.params.tableName} Time ${Date.now() - start}ms`)
  res.send(r);
});
router.get('/api/raw/personas', async (req, res, next) => {
  const start = Date.now();
  let r = await query(`SELECT * FROM personas WHERE nombre LIKE '%A' LIMIT 1000;`)
  let count = await query(`SELECT COUNT(*) FROM personas WHERE nombre LIKE '%A'; `);
  console.log(`api/raw/personas Time ${Date.now() - start}ms`);
  res.send({ rows: r.rows, count: count.rows[0].count });
});

export default router;