import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class Example {
    static final class RespException extends RuntimeException {
        RespException(String message) {
            super(message);
        }
    }

    public static void main(String[] args) throws Exception {
        String bin = System.getenv().getOrDefault("ANTE_BIN", "ante");
        Path dir = Files.createTempDirectory("ante-embed-java-");
        Process process = new ProcessBuilder(
                bin, "serve",
                "--db", dir.resolve("ante.redb").toString(),
                "--no-amqp",
                "--native", "127.0.0.1:0")
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();

        try {
            String address = awaitAddress(process.getInputStream());
            int split = address.lastIndexOf(':');
            String host = address.substring(0, split);
            int port = Integer.parseInt(address.substring(split + 1));

            try (Socket socket = new Socket(host, port)) {
                socket.setSoTimeout(10_000);
                InputStream in = new BufferedInputStream(socket.getInputStream());
                OutputStream out = socket.getOutputStream();

                System.out.println("hello " + request(in, out, "HELLO", "1"));
                System.out.println("declare " + request(in, out, "DECLARE", "demo"));

                byte[] payload = "{\"lang\":\"java\",\"n\":1}".getBytes(StandardCharsets.UTF_8);
                System.out.println("publish " + request(
                        in, out, "PUBLISH", "demo", payload, "application/json"));

                List<?> records = (List<?>) request(
                        in, out, "TAKE", "demo", "1", "30000", "5000");
                List<?> record = (List<?>) records.get(0);
                String token = text(record.get(0));
                System.out.println("took {token=" + token
                        + ", messageId=" + text(record.get(1))
                        + ", attempts=" + text(record.get(5))
                        + ", payload=" + text(record.get(6)) + "}");

                System.out.println("ack " + request(in, out, "ACK", token));
                List<?> info = (List<?>) request(in, out, "INFO", "demo");
                List<String> fields = new ArrayList<>();
                for (Object value : info) {
                    fields.add(text(value));
                }
                System.out.println("info " + String.join(" ", fields));
            }
        } finally {
            process.destroy();
            if (!process.waitFor(6, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
            deleteTree(dir);
        }
    }

    static String awaitAddress(InputStream stdout) throws Exception {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            int value = stdout.read();
            if (value < 0) {
                throw new IllegalStateException("ante exited during startup");
            }
            if (value == '\n') {
                String text = line.toString(StandardCharsets.UTF_8).strip();
                line.reset();
                if (text.startsWith("native ")) {
                    return text.substring("native ".length()).strip();
                }
            } else if (value != '\r') {
                line.write(value);
            }
        }
        throw new IllegalStateException("startup timed out");
    }

    static Object request(InputStream in, OutputStream out, Object... parts) throws Exception {
        out.write(encode(parts));
        out.flush();
        return readValue(in);
    }

    static byte[] encode(Object... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeAscii(out, "*" + parts.length + "\r\n");
        for (Object part : parts) {
            byte[] body = part instanceof byte[] bytes
                    ? bytes
                    : part.toString().getBytes(StandardCharsets.UTF_8);
            writeAscii(out, "$" + body.length + "\r\n");
            out.writeBytes(body);
            writeAscii(out, "\r\n");
        }
        return out.toByteArray();
    }

    static Object readValue(InputStream in) throws Exception {
        int kind = in.read();
        if (kind < 0) {
            throw new RespException("connection closed");
        }
        String line = readLine(in);
        return switch (kind) {
            case '+' -> line;
            case '-' -> throw new RespException(line);
            case ':' -> Long.parseLong(line);
            case '$' -> readBulk(in, Integer.parseInt(line));
            case '*' -> readArray(in, Integer.parseInt(line));
            default -> throw new RespException("unsupported RESP byte " + kind);
        };
    }

    static byte[] readBulk(InputStream in, int length) throws Exception {
        if (length == -1) {
            return null;
        }
        byte[] body = in.readNBytes(length + 2);
        if (body.length != length + 2) {
            throw new RespException("truncated bulk reply");
        }
        return Arrays.copyOf(body, length);
    }

    static List<Object> readArray(InputStream in, int count) throws Exception {
        if (count == -1) {
            return null;
        }
        List<Object> items = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            items.add(readValue(in));
        }
        return items;
    }

    static String readLine(InputStream in) throws Exception {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        while (true) {
            int value = in.read();
            if (value < 0) {
                throw new RespException("connection closed");
            }
            if (value == '\r') {
                int next = in.read();
                if (next != '\n') {
                    throw new RespException("malformed line ending");
                }
                return line.toString(StandardCharsets.UTF_8);
            }
            line.write(value);
        }
    }

    static String text(Object value) {
        return value instanceof byte[] bytes
                ? new String(bytes, StandardCharsets.UTF_8)
                : String.valueOf(value);
    }

    static void writeAscii(ByteArrayOutputStream out, String text) {
        out.writeBytes(text.getBytes(StandardCharsets.UTF_8));
    }

    static void deleteTree(Path root) {
        try (var paths = Files.walk(root)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
            });
        } catch (Exception ignored) {
        }
    }
}