// Базовый URL API
const API_URL = "https://api.green-api.com";

// Элементы UI
const idInstanceInput = document.getElementById('idInstance');
const apiTokenInput = document.getElementById('apiTokenInstance');
const responseArea = document.getElementById('responseArea');

// Вспомогательная функция для вывода ответа в текстовое поле
function showResponse(data) {
    // Форматируем JSON с отступами (4 пробела) для красоты
    responseArea.value = JSON.stringify(data, null, 4);
}

// Вспомогательная функция для формирования URL запроса
function buildUrl(method) {
    const idInstance = idInstanceInput.value.trim();
    const apiTokenInstance = apiTokenInput.value.trim();
    
    if (!idInstance || !apiTokenInstance) {
        alert("Пожалуйста, заполните idInstance и ApiTokenInstance");
        return null;
    }
    
    return `${API_URL}/waInstance${idInstance}/${method}/${apiTokenInstance}`;
}

// 1. getSettings
document.getElementById('btn-getSettings').addEventListener('click', async () => {
    const url = buildUrl('getSettings');
    if (!url) return;

    try {
        const response = await fetch(url);
        const data = await response.json();
        showResponse(data);
    } catch (error) {
        showResponse({ error: error.message });
    }
});

// 2. getStateInstance
document.getElementById('btn-getStateInstance').addEventListener('click', async () => {
    const url = buildUrl('getStateInstance');
    if (!url) return;

    try {
        const response = await fetch(url);
        const data = await response.json();
        showResponse(data);
    } catch (error) {
        showResponse({ error: error.message });
    }
});

// 3. sendMessage
document.getElementById('btn-sendMessage').addEventListener('click', async () => {
    const url = buildUrl('sendMessage');
    if (!url) return;

    const phone = document.getElementById('phoneMessage').value.trim();
    const message = document.getElementById('textMessage').value;

    if (!phone || !message) {
        alert("Введите номер телефона и сообщение");
        return;
    }

    // Формируем payload. GREEN-API требует формат номера: 79991234567@c.us
    const payload = {
        chatId: `${phone}@c.us`,
        message: message
    };

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await response.json();
        showResponse(data);
    } catch (error) {
        showResponse({ error: error.message });
    }
});

// 4. sendFileByUrl
document.getElementById('btn-sendFileByUrl').addEventListener('click', async () => {
    const url = buildUrl('sendFileByUrl');
    if (!url) return;

    const phone = document.getElementById('phoneFile').value.trim();
    const fileUrl = document.getElementById('urlFile').value.trim();

    if (!phone || !fileUrl) {
        alert("Введите номер телефона и URL файла");
        return;
    }

    // Достаем имя файла из URL (например: horse.png)
    const fileName = fileUrl.substring(fileUrl.lastIndexOf('/') + 1) || "file";

    const payload = {
        chatId: `${phone}@c.us`,
        urlFile: fileUrl,
        fileName: fileName
    };

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await response.json();
        showResponse(data);
    } catch (error) {
        showResponse({ error: error.message });
    }
});