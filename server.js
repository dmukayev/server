// server.js

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const multer = require('multer'); // Для обработки загрузки файлов
const path = require('path');
const { spawn } = require('child_process');
const fs = require('fs');
const http = require('http');
const socketIo = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: '*', // Настройте согласно вашим требованиям
    methods: ['GET', 'POST']
  }
});

// Middleware
app.use(cors()); // Разрешаем запросы из другого порта (React-приложения)
app.use(express.json()); // Для парсинга JSON-тел запросов

// Настройка подключения к PostgreSQL
const pool = new Pool({
  user: 'postgres',
  password: 'Sputnik111',
  host: 'localhost',
  port: 5433,
  database: 'postgres' // Укажите вашу базу данных
});

// Настройка Multer для загрузки файлов
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/'); // Файлы сохраняются в папке 'uploads'
  },
  filename: (req, file, cb) => {
    cb(null, file.originalname); // Сохраняем файл с оригинальным именем
  }
});

const fileFilter = (req, file, cb) => {
  const allowedMimeTypes = [
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', // .xlsx
    'application/vnd.ms-excel' // .xls
  ];

  console.log(`Загружаемый файл: ${file.originalname}, MIME-тип: ${file.mimetype}`);

  if (allowedMimeTypes.includes(file.mimetype)) {
    cb(null, true);
  } else {
    cb(new Error('Только Excel-файлы разрешены!'));
  }
};

const upload = multer({
  storage: storage,
  limits: { fileSize: 40 * 1024 * 1024 }, // 40MB
  fileFilter: fileFilter
});

// Создаём папку 'uploads', если она ещё не существует
const uploadDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir);
  console.log("Папка 'uploads/' создана.");
}

// Определите путь к Python из виртуального окружения (или системному Python)
const venvPath = path.join(__dirname, 'venv', 'bin', 'python3'); // Для Unix/Mac
// const venvPath = path.join(__dirname, 'venv', 'Scripts', 'python.exe'); // Для Windows

// Проверка наличия виртуального окружения и Python
if (!fs.existsSync(venvPath)) {
  console.error(`Python-исполнитель не найден по пути: ${venvPath}`);
  process.exit(1);
}

// ------------------- Эндпоинты для медпредов и продуктов -------------------

// Получение списка всех медпредов
app.get('/api/medpreds', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, medpred_name FROM medpreds ORDER BY medpred_name');
    res.json(result.rows);
  } catch (err) {
    console.error('Ошибка при получении медпредов:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Получение продуктов, привязанных к конкретному медпреду
app.get('/api/medpreds/:id/products', async (req, res) => {
  const medpredId = req.params.id;
  try {
    const result = await pool.query(`
      SELECT mp.id, mp.product_name, mp.product_code, mp.assigned_date
      FROM medpred_products mp
      WHERE mp.medpred_id = $1
      ORDER BY mp.product_name
    `, [medpredId]);
    res.json(result.rows);
  } catch (err) {
    console.error(`Ошибка при получении продуктов для медпреда ${medpredId}:`, err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Поиск продуктов по названию (для автокомплита или поиска)
app.get('/api/products', async (req, res) => {
  const search = req.query.search || '';
  try {
    const result = await pool.query(`
      SELECT product_name, product_code
      FROM products
      WHERE product_name ILIKE $1
      ORDER BY product_name
      LIMIT 10
    `, [`%${search}%`]);
    res.json(result.rows);
  } catch (err) {
    console.error('Ошибка при поиске продуктов:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Привязка продукта к медпреду
app.post('/api/medpred-products', async (req, res) => {
  try {
    const { medpred_id, product_name } = req.body;

    if (!medpred_id || !product_name) {
      return res.status(400).json({ error: 'medpred_id и product_name обязательны' });
    }

    // Проверка наличия медпреда
    const medpredCheck = await pool.query(`
      SELECT id FROM medpreds WHERE id = $1
    `, [medpred_id]);

    if (medpredCheck.rowCount === 0) {
      return res.status(404).json({ error: 'Medpred не найден' });
    }

    // Получение информации о продукте
    const productCheck = await pool.query(`
      SELECT id, product_code FROM products WHERE product_name = $1
    `, [product_name]);

    if (productCheck.rowCount === 0) {
      return res.status(404).json({ error: 'Продукт не найден' });
    }

    const { id: product_id, product_code } = productCheck.rows[0];

    // Проверка, есть ли уже привязка
    const existing = await pool.query(`
      SELECT * FROM medpred_products
      WHERE medpred_id = $1 AND product_name = $2
    `, [medpred_id, product_name]);

    if (existing.rowCount > 0) {
      return res.status(400).json({ error: 'Продукт уже привязан к этому медпреду' });
    }

    // Вставка записи
    await pool.query(`
      INSERT INTO medpred_products (medpred_id, product_id, product_name, product_code, assigned_date)
      VALUES ($1, $2, $3, $4, NOW())
    `, [medpred_id, product_id, product_name, product_code]);

    res.status(201).json({ message: 'Продукт успешно привязан к медпреду' });
  } catch (err) {
    console.error('Ошибка при добавлении продукта к медпреду:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Отвязка продукта от медпреда
app.delete('/api/medpred-products', async (req, res) => {
  try {
    const { medpred_id, product_name } = req.body;

    if (!medpred_id || !product_name) {
      return res.status(400).json({ error: 'medpred_id и product_name обязательны' });
    }

    // Удаляем запись
    const result = await pool.query(`
      DELETE FROM medpred_products
      WHERE medpred_id = $1 AND product_name = $2
    `, [medpred_id, product_name]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Связь не найдена' });
    }

    res.json({ message: 'Продукт успешно отвязан от медпреда' });
  } catch (err) {
    console.error('Ошибка при удалении привязки продукта к медпреду:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// ------------------- Эндпоинты для загрузки файлов -------------------

// Загрузка Ассортиментного плана
app.post('/api/upload-assortment-plan', upload.single('file'), (req, res) => {
  const file = req.file;
  if (!file) {
    return res.status(400).json({ error: 'Файл не был загружен' });
  }

  const pythonScript = 'upload_data.py'; // Убедитесь, что скрипт существует
  const filePath = path.join(__dirname, 'uploads', file.filename);

  console.log(`Запуск скрипта ${pythonScript} для файла ${filePath}`);

  const process = spawn(venvPath, [pythonScript, filePath, 'assortment_plan']);

  let stdout = '';
  let stderr = '';

  process.stdout.on('data', (data) => {
    stdout += data.toString();
    //console.log(`stdout: ${data.toString()}`);
  });

  process.stderr.on('data', (data) => {
    stderr += data.toString();
    console.error(`stderr: ${data.toString()}`);
  });

  process.on('close', (code) => {
    if (code !== 0) {
      console.error(`Ошибка при выполнении скрипта ${pythonScript}: ${stderr}`);
      return res.status(500).json({ error: 'Ошибка при обработке файла' });
    }
    console.log(`Скрипт ${pythonScript} завершился успешно.`);
    res.json({ message: 'Ассортиментный план успешно загружен и обработан' });
  });
}, (error, req, res, next) => {
  // Обработка ошибок Multer
  if (error instanceof multer.MulterError) {
    return res.status(400).json({ error: error.message });
  } else if (error) {
    return res.status(400).json({ error: error.message });
  }
  next();
});

// Загрузка Заявки (application)
app.post('/api/upload-application', upload.single('file'), (req, res) => {
  const file = req.file;
  if (!file) {
    return res.status(400).json({ error: 'Файл не был загружен' });
  }

  const pythonScript = 'upload_zayavka.py'; // Убедитесь, что скрипт существует
  const filePath = path.join(__dirname, 'uploads', file.filename);

  console.log(`Запуск скрипта ${pythonScript} для файла ${filePath}`);

  const process = spawn(venvPath, [pythonScript, filePath]);

  let stdout = '';
  let stderr = '';

  process.stdout.on('data', (data) => {
    stdout += data.toString();
    console.log(`stdout: ${data.toString()}`);
  });

  process.stderr.on('data', (data) => {
    stderr += data.toString();
    console.error(`stderr: ${data.toString()}`);
  });

  process.on('close', (code) => {
    if (code !== 0) {
      console.error(`Ошибка при выполнении скрипта ${pythonScript}: ${stderr}`);
      return res.status(500).json({ error: 'Ошибка при обработке файла' });
    }
    console.log(`Скрипт ${pythonScript} завершился успешно.`);
    res.json({ message: 'Заявка успешно загружена и обработана' });
  });
}, (error, req, res, next) => {
  if (error instanceof multer.MulterError) {
    return res.status(400).json({ error: error.message });
  } else if (error) {
    return res.status(400).json({ error: error.message });
  }
  next();
});

// ------------------- Эндпоинт "Начать проверку" с WebSocket -------------------

io.on('connection', (socket) => {
  console.log('Клиент подключен:', socket.id);

  socket.on('disconnect', () => {
    console.log('Клиент отключен:', socket.id);
  });
});

app.post('/api/run-check-zayavka', (req, res) => {
  // Отправляем клиенту подтверждение запуска
  res.json({ message: 'Проверка начата' });

  const pythonScript = 'CheckZayavka.py';

  console.log(`Запуск скрипта ${pythonScript}`);

  // Запускаем скрипт через spawn для потокового чтения
  const process = spawn(venvPath, [pythonScript]);

  process.stdout.setEncoding('utf8');

  process.stdout.on('data', (data) => {
    //console.log(`stdout: ${data.toString()}`);
    // Предполагается, что каждая строка - это отдельный JSON объект
    const lines = data.split('\n').filter(line => line.trim() !== '');
    lines.forEach(line => {
      try {
        const parsed = JSON.parse(line);
        //console.log(`Отправка данных клиентам: ${JSON.stringify(parsed)}`);
        io.emit('check-result', parsed); // Отправляем всем подключённым клиентам
      } catch (err) {
        console.error('Ошибка парсинга JSON из скрипта:', err);
      }
    });
  });

  let stderr = '';
  process.stderr.on('data', (data) => {
    stderr += data.toString();
    console.error(`stderr: ${data.toString()}`);
  });

  process.on('close', (code) => {
    if (code !== 0) {
      console.error(`Ошибка при запуске скрипта ${pythonScript}: ${stderr}`);
      io.emit('check-error', { error: 'Ошибка при выполнении CheckZayavka.py', details: stderr });
    } else {
      console.log('Скрипт CheckZayavka.py завершился успешно.');
      io.emit('check-complete');
    }
  });
});

// ------------------- Эндпоинты для получения данных медпредов -------------------

// Получение списка всех медпредов
app.get('/api/medpreds', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, medpred_name FROM medpreds ORDER BY medpred_name');
    res.json(result.rows);
  } catch (err) {
    console.error('Ошибка при получении медпредов:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Получение продуктов, привязанных к конкретному медпреду
app.get('/api/medpreds/:id/products', async (req, res) => {
  const medpredId = req.params.id;
  try {
    const result = await pool.query(`
      SELECT mp.id, mp.product_name, mp.product_code, mp.assigned_date
      FROM medpred_products mp
      WHERE mp.medpred_id = $1
      ORDER BY mp.product_name
    `, [medpredId]);
    res.json(result.rows);
  } catch (err) {
    console.error(`Ошибка при получении продуктов для медпреда ${medpredId}:`, err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Поиск продуктов по названию (для автокомплита или поиска)
app.get('/api/products', async (req, res) => {
  const search = req.query.search || '';
  try {
    const result = await pool.query(`
      SELECT product_name, product_code
      FROM products
      WHERE product_name ILIKE $1
      ORDER BY product_name
      LIMIT 10
    `, [`%${search}%`]);
    res.json(result.rows);
  } catch (err) {
    console.error('Ошибка при поиске продуктов:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Привязка продукта к медпреду
app.post('/api/medpred-products', async (req, res) => {
  try {
    const { medpred_id, product_name } = req.body;

    if (!medpred_id || !product_name) {
      return res.status(400).json({ error: 'medpred_id и product_name обязательны' });
    }

    // Проверка наличия медпреда
    const medpredCheck = await pool.query(`
      SELECT id FROM medpreds WHERE id = $1
    `, [medpred_id]);

    if (medpredCheck.rowCount === 0) {
      return res.status(404).json({ error: 'Medpred не найден' });
    }

    // Получение информации о продукте
    const productCheck = await pool.query(`
      SELECT id, product_code FROM products WHERE product_name = $1
    `, [product_name]);

    if (productCheck.rowCount === 0) {
      return res.status(404).json({ error: 'Продукт не найден' });
    }

    const { id: product_id, product_code } = productCheck.rows[0];

    // Проверка, есть ли уже привязка
    const existing = await pool.query(`
      SELECT * FROM medpred_products
      WHERE medpred_id = $1 AND product_name = $2
    `, [medpred_id, product_name]);

    if (existing.rowCount > 0) {
      return res.status(400).json({ error: 'Продукт уже привязан к этому медпреду' });
    }

    // Вставка записи
    await pool.query(`
      INSERT INTO medpred_products (medpred_id, product_id, product_name, product_code, assigned_date)
      VALUES ($1, $2, $3, $4, NOW())
    `, [medpred_id, product_id, product_name, product_code]);

    res.status(201).json({ message: 'Продукт успешно привязан к медпреду' });
  } catch (err) {
    console.error('Ошибка при добавлении продукта к медпреду:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// Отвязка продукта от медпреда
app.delete('/api/medpred-products', async (req, res) => {
  try {
    const { medpred_id, product_name } = req.body;

    if (!medpred_id || !product_name) {
      return res.status(400).json({ error: 'medpred_id и product_name обязательны' });
    }

    // Удаляем запись
    const result = await pool.query(`
      DELETE FROM medpred_products
      WHERE medpred_id = $1 AND product_name = $2
    `, [medpred_id, product_name]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Связь не найдена' });
    }

    res.json({ message: 'Продукт успешно отвязан от медпреда' });
  } catch (err) {
    console.error('Ошибка при удалении привязки продукта к медпреду:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// ============ ВАЖНО! Новый эндпоинт для запуска CheckZayavka.py ============
app.post('/api/run-check-zayavka', (req, res) => {
  // Отправляем клиенту подтверждение запуска
  res.json({ message: 'Проверка начата' });

  const pythonScript = 'CheckZayavka.py';

  console.log(`Запуск скрипта ${pythonScript}`);

  // Запускаем скрипт через spawn для потокового чтения
  const process = spawn(venvPath, [pythonScript]);

  process.stdout.setEncoding('utf8');

  process.stdout.on('data', (data) => {
    //console.log(`stdout: ${data.toString()}`);
    // Предполагается, что каждая строка - это отдельный JSON объект
    const lines = data.split('\n').filter(line => line.trim() !== '');
    lines.forEach(line => {
      try {
        const parsed = JSON.parse(line);
        //console.log(`Отправка данных клиентам: ${JSON.stringify(parsed)}`);
        io.emit('check-result', parsed); // Отправляем всем подключённым клиентам
      } catch (err) {
        console.error('Ошибка парсинга JSON из скрипта:', err);
      }
    });
  });

  let stderr = '';
  process.stderr.on('data', (data) => {
    stderr += data.toString();
    console.error(`stderr: ${data.toString()}`);
  });

  process.on('close', (code) => {
    if (code !== 0) {
      console.error(`Ошибка при запуске скрипта ${pythonScript}: ${stderr}`);
      io.emit('check-error', { error: 'Ошибка при выполнении CheckZayavka.py', details: stderr });
    } else {
      console.log('Скрипт CheckZayavka.py завершился успешно.');
      io.emit('check-complete');
    }
  });
});

// ------------------- Эндпоинты для загрузки файлов -------------------

// Загрузка Ассортиментного плана
app.post('/api/upload-assortment-plan', upload.single('file'), (req, res) => {
  const file = req.file;
  if (!file) {
    return res.status(400).json({ error: 'Файл не был загружен' });
  }

  const pythonScript = 'upload_data.py'; // Убедитесь, что скрипт существует
  const filePath = path.join(__dirname, 'uploads', file.filename);

  console.log(`Запуск скрипта ${pythonScript} для файла ${filePath}`);

  const process = spawn(venvPath, [pythonScript, filePath, 'assortment_plan']);

  let stdout = '';
  let stderr = '';

  process.stdout.on('data', (data) => {
    stdout += data.toString();
    console.log(`stdout: ${data.toString()}`);
  });

  process.stderr.on('data', (data) => {
    stderr += data.toString();
    console.error(`stderr: ${data.toString()}`);
  });

  process.on('close', (code) => {
    if (code !== 0) {
      console.error(`Ошибка при выполнении скрипта ${pythonScript}: ${stderr}`);
      return res.status(500).json({ error: 'Ошибка при обработке файла' });
    }
    console.log(`Скрипт ${pythonScript} завершился успешно.`);
    res.json({ message: 'Ассортиментный план успешно загружен и обработан' });
  });
}, (error, req, res, next) => {
  // Обработка ошибок Multer
  if (error instanceof multer.MulterError) {
    return res.status(400).json({ error: error.message });
  } else if (error) {
    return res.status(400).json({ error: error.message });
  }
  next();
});

// Загрузка Заявки (application)
app.post('/api/upload-application', upload.single('file'), (req, res) => {
  const file = req.file;
  if (!file) {
    return res.status(400).json({ error: 'Файл не был загружен' });
  }

  const pythonScript = 'upload_zayavka.py'; // Убедитесь, что скрипт существует
  const filePath = path.join(__dirname, 'uploads', file.filename);

  console.log(`Запуск скрипта ${pythonScript} для файла ${filePath}`);

  const process = spawn(venvPath, [pythonScript, filePath]);

  let stdout = '';
  let stderr = '';

  process.stdout.on('data', (data) => {
    stdout += data.toString();
    console.log(`stdout: ${data.toString()}`);
  });

  process.stderr.on('data', (data) => {
    stderr += data.toString();
    console.error(`stderr: ${data.toString()}`);
  });

  process.on('close', (code) => {
    if (code !== 0) {
      console.error(`Ошибка при выполнении скрипта ${pythonScript}: ${stderr}`);
      return res.status(500).json({ error: 'Ошибка при обработке файла' });
    }
    console.log(`Скрипт ${pythonScript} завершился успешно.`);
    res.json({ message: 'Заявка успешно загружена и обработана' });
  });
}, (error, req, res, next) => {
  if (error instanceof multer.MulterError) {
    return res.status(400).json({ error: error.message });
  } else if (error) {
    return res.status(400).json({ error: error.message });
  }
  next();
});


// ------------------- Эндпоинт для получения данных МП Заявка -------------------
app.get('/api/mpzayavka', async (req, res) => {
  try {
    const query = `
      SELECT
        m.medpred_name,                   -- Replaced medpred_id with medpred_name
        mp.product_name AS medpred_product_name,
        p.department_group,
        p.current_stock,
        p.sales_rate,
        p.abc_category,
        p.xyz_category,
        p.profit_sum,
        p.status
      FROM
        public.medpred_products mp
      INNER JOIN
        public.products p
        ON LOWER(mp.product_name) = LOWER(p.product_name) -- Case-insensitive join on product_name
      INNER JOIN
        public.medpreds m
        ON mp.medpred_id = m.id
      ORDER BY
        m.medpred_name ASC,
        p.department_group ASC;
    `;
    
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Ошибка при получении данных МП Заявка:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});



// Эндпоинт для Ассортиментного плана
app.get('/api/assortment-plan', async (req, res) => {
  try {
    const query = `
WITH ProfitData AS (
    SELECT 
        product_name,
        department_group,
        profit_sum,
        status,
        current_stock,
        sales_rate,
        max_stock,
        order_point_stock,
        abc_category,
        xyz_category
    FROM products
)
SELECT 
    product_name AS "Продукт",
    
    -- А. Бокейхана 32
    SUM(profit_sum) FILTER (WHERE department_group = 'А. Бокейхана 32') AS "А. Бокейхана 32 - Сумма прибыли",
    MAX(status) FILTER (WHERE department_group = 'А. Бокейхана 32') AS "А. Бокейхана 32 - Статус",
    SUM(current_stock) FILTER (WHERE department_group = 'А. Бокейхана 32') AS "А. Бокейхана 32 - Текущий запас",
    ROUND(AVG(sales_rate) FILTER (WHERE department_group = 'А. Бокейхана 32'), 2) AS "А. Бокейхана 32 - Скорость продаж",
    MAX(max_stock) FILTER (WHERE department_group = 'А. Бокейхана 32') AS "А. Бокейхана 32 - Максимальный запас",
    SUM(order_point_stock) FILTER (WHERE department_group = 'А. Бокейхана 32') AS "А. Бо - Точ",
    MAX(abc_category) FILTER (WHERE department_group = 'А. Бокейхана 32') AS "А. Бокейхана 32 - Категория ABC",
    MAX(xyz_category) FILTER (WHERE department_group = 'А. Бокейхана 32') AS "А. Бокейхана 32 - Категория XYZ",
    
    -- Асфендиярова 2
    SUM(profit_sum) FILTER (WHERE department_group = 'Асфендиярова 2') AS "Асфендиярова 2 - Сумма прибыли",
    MAX(status) FILTER (WHERE department_group = 'Асфендиярова 2') AS "Асфендиярова 2 - Статус",
    SUM(current_stock) FILTER (WHERE department_group = 'Асфендиярова 2') AS "Асфендиярова 2 - Текущий запас",
    ROUND(AVG(sales_rate) FILTER (WHERE department_group = 'Асфендиярова 2'), 2) AS "Асфендиярова 2 - Скорость продаж",
    MAX(max_stock) FILTER (WHERE department_group = 'Асфендиярова 2') AS "Асф - Мак",
    SUM(order_point_stock) FILTER (WHERE department_group = 'Асфендиярова 2') AS "Ас - Точка",
    MAX(abc_category) FILTER (WHERE department_group = 'Асфендиярова 2') AS "Асфендиярова 2 - Категория ABC",
    MAX(xyz_category) FILTER (WHERE department_group = 'Асфендиярова 2') AS "Асфендиярова 2 - Категория XYZ",
    
    -- Жумабаева 3
    SUM(profit_sum) FILTER (WHERE department_group = 'Жумабаева 3') AS "Жумабаева 3 - Сумма прибыли",
    MAX(status) FILTER (WHERE department_group = 'Жумабаева 3') AS "Жумабаева 3 - Статус",
    SUM(current_stock) FILTER (WHERE department_group = 'Жумабаева 3') AS "Жумабаева 3 - Текущий запас",
    ROUND(AVG(sales_rate) FILTER (WHERE department_group = 'Жумабаева 3'), 2) AS "Жумабаева 3 - Скорость продаж",
    MAX(max_stock) FILTER (WHERE department_group = 'Жумабаева 3') AS "Жумабаева 3 - Максимальный запас",
    SUM(order_point_stock) FILTER (WHERE department_group = 'Жумабаева 3') AS "Жумабаева 3 - Точка заказа запаса",
    MAX(abc_category) FILTER (WHERE department_group = 'Жумабаева 3') AS "Жумабаева 3 - Категория ABC",
    MAX(xyz_category) FILTER (WHERE department_group = 'Жумабаева 3') AS "Жумабаева 3 - Категория XYZ",
    
    -- Пушкина 1
    SUM(profit_sum) FILTER (WHERE department_group = 'Пушкина 1') AS "Пушкина 1 - Сумма прибыли",
    MAX(status) FILTER (WHERE department_group = 'Пушкина 1') AS "Пушкина 1 - Статус",
    SUM(current_stock) FILTER (WHERE department_group = 'Пушкина 1') AS "Пушкина 1 - Текущий запас",
    ROUND(AVG(sales_rate) FILTER (WHERE department_group = 'Пушкина 1'), 2) AS "Пушкина 1 - Скорость продаж",
    MAX(max_stock) FILTER (WHERE department_group = 'Пушкина 1') AS "Пушкина 1 - Максимальный запас",
    SUM(order_point_stock) FILTER (WHERE department_group = 'Пушкина 1') AS "Пушкина 1 - Точка заказа запаса",
    MAX(abc_category) FILTER (WHERE department_group = 'Пушкина 1') AS "Пушкина 1 - Категория ABC",
    MAX(xyz_category) FILTER (WHERE department_group = 'Пушкина 1') AS "Пушкина 1 - Категория XYZ",
    
    -- Общая сумма прибыли
    SUM(profit_sum) AS "Общая сумма прибыли"
FROM 
    ProfitData
GROUP BY 
    product_name
ORDER BY 
    "Общая сумма прибыли" DESC;

    `;

    const result = await pool.query(query);
    // Возвращаем результат в формате JSON
    res.json(result.rows);
  } catch (error) {
    console.error('Ошибка при получении данных для Ассортиментного плана:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});


// ЭНДПОИНТ ПЕРЕБРОСКИ
app.get('/api/transfers', async (req, res) => {
  try {
    const query = `
      SELECT
          -- Общая стоимость запаса у донора (для наглядности)
          (d.current_stock * d.average_purchase_price) AS donor_inventory_value,
          d.product_name            AS product_name,
          d.department_group        AS donor_pharmacy,
          d.sales_rate              AS donor_sales_rate,
          d.current_stock           AS donor_current_stock,
          r.department_group        AS receiver_pharmacy,
          r.sales_rate              AS receiver_sales_rate,
          r.current_stock           AS receiver_current_stock,
          FLOOR(
            LEAST(
              CASE 
                WHEN d.sales_rate IS NULL THEN (d.current_stock - 1)
                ELSE (d.current_stock - 2*d.sales_rate)
              END,
              (2*r.sales_rate - r.current_stock)
            )
          ) AS transfer_qty
      FROM products AS d
      JOIN products AS r
        ON d.product_code = r.product_code
           AND d.department_group <> r.department_group
      WHERE 
        CASE 
          WHEN d.sales_rate IS NULL THEN (d.current_stock - 1)
          ELSE (d.current_stock - 2*d.sales_rate)
        END > 0
        AND r.sales_rate IS NOT NULL
        AND (2*r.sales_rate - r.current_stock) > 0
        AND FLOOR(
              LEAST(
                CASE 
                  WHEN d.sales_rate IS NULL THEN (d.current_stock - 1)
                  ELSE (d.current_stock - 2*d.sales_rate)
                END,
                (2*r.sales_rate - r.current_stock)
              )
            ) >= 1
      ORDER BY
          donor_inventory_value DESC;
    `;

    const result = await pool.query(query);
    res.json(result.rows); 
  } catch (err) {
    console.error('Ошибка при получении логики перебросок:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});



// Запуск сервера
const PORT = 4000;
server.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
