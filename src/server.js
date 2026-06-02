require("dotenv").config(); // Load environment variables from .env so PORT and other runtime settings can be configured outside code

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const uploadRoute = require('./routes/upload.route');

const app = express();
const server = http.createServer(app);

const io = new Server(server);

app.use('/api', uploadRoute);
app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);
});

const PORT = process.env.PORT 

server.listen(PORT, () => {
  console.log(`SentinelScale running on port ${PORT}`);
});
