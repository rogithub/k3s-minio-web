const express = require('express');
const { Client } = require('minio');
const cors = require('cors');

const app = express();
app.use(cors());

// Configuración de MinIO
const minioClient = new Client({
  endPoint: process.env.MINIO_ENDPOINT || 'minio.minio-system.svc.cluster.local',
  port: parseInt(process.env.MINIO_PORT) || 9000,
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY
});

// Buckets permitidos para lectura pública
const PUBLIC_READ_BUCKETS = process.env.PUBLIC_BUCKETS 
  ? process.env.PUBLIC_BUCKETS.split(',') 
  : ['material-didactico', 'papeleria-fotos-productos'];

// Middleware para validar buckets permitidos
function validateBucket(req, res, next) {
  const { bucket } = req.params;
  if (!PUBLIC_READ_BUCKETS.includes(bucket)) {
    return res.status(403).json({ error: 'Bucket no permitido' });
  }
  next();
}

// Endpoint para obtener un objeto
app.get('/:bucket/:object(*)', validateBucket, async (req, res) => {
  const { bucket, object } = req.params;
  
  try {
    // Verificar si el objeto existe
    const stat = await minioClient.statObject(bucket, object);
    
    // Configurar headers apropiados
    res.setHeader('Content-Type', stat.metaData['content-type'] || 'application/octet-stream');
    res.setHeader('Content-Length', stat.size);
    res.setHeader('ETag', stat.etag);
    res.setHeader('Cache-Control', 'public, max-age=31536000');
    
    // Stream del objeto
    const dataStream = await minioClient.getObject(bucket, object);
    dataStream.pipe(res);
    
    dataStream.on('error', (err) => {
      console.error('Error streaming object:', err);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Error al transmitir el archivo' });
      }
    });
    
  } catch (err) {
    console.error('Error getting object:', err);
    if (err.code === 'NoSuchKey') {
      return res.status(404).json({ error: 'Archivo no encontrado' });
    }
    res.status(500).json({ error: 'Error al obtener el archivo' });
  }
});

// Endpoint para listar objetos (opcional)
app.get('/:bucket', validateBucket, async (req, res) => {
  const { bucket } = req.params;
  const prefix = req.query.prefix || '';
  
  try {
    const objects = [];
    const stream = minioClient.listObjectsV2(bucket, prefix, true);
    
    stream.on('data', (obj) => {
      objects.push({
        name: obj.name,
        size: obj.size,
        lastModified: obj.lastModified,
        etag: obj.etag
      });
    });
    
    stream.on('end', () => {
      res.json({ bucket, prefix, objects });
    });
    
    stream.on('error', (err) => {
      console.error('Error listing objects:', err);
      res.status(500).json({ error: 'Error al listar objetos' });
    });
    
  } catch (err) {
    console.error('Error listing bucket:', err);
    res.status(500).json({ error: 'Error al listar el bucket' });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`MinIO proxy listening on port ${PORT}`);
  console.log(`Public read buckets: ${PUBLIC_READ_BUCKETS.join(', ')}`);
});