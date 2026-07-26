package async_http_server

import "core:path/filepath"
import "core:strings"

init_mime_types :: proc(mime_types: ^map[string]string) {
	mime_types[".html"] = "text/html; charset=utf-8"
	mime_types[".htm"] = "text/html; charset=utf-8"
	mime_types[".css"] = "text/css; charset=utf-8"
	mime_types[".js"] = "text/javascript; charset=utf-8"
	mime_types[".mjs"] = "text/javascript; charset=utf-8"
	mime_types[".json"] = "application/json"
	mime_types[".xml"] = "application/xml"
	mime_types[".txt"] = "text/plain; charset=utf-8"
	mime_types[".csv"] = "text/csv"
	mime_types[".pdf"] = "application/pdf"
	mime_types[".zip"] = "application/zip"
	mime_types[".wasm"] = "application/wasm"

	mime_types[".yaml"] = "application/yaml"
	mime_types[".yml"] = "application/yaml"
	mime_types[".toml"] = "application/toml"

	mime_types[".png"] = "image/png"
	mime_types[".jpg"] = "image/jpeg"
	mime_types[".jpeg"] = "image/jpeg"
	mime_types[".gif"] = "image/gif"
	mime_types[".webp"] = "image/webp"
	mime_types[".avif"] = "image/avif"
	mime_types[".svg"] = "image/svg+xml"
	mime_types[".ico"] = "image/x-icon"
	mime_types[".bmp"] = "image/bmp"

	mime_types[".woff"] = "font/woff"
	mime_types[".woff2"] = "font/woff2"
	mime_types[".ttf"] = "font/ttf"
	mime_types[".otf"] = "font/otf"

	mime_types[".mp3"] = "audio/mpeg"
	mime_types[".wav"] = "audio/wav"
	mime_types[".ogg"] = "audio/ogg"
	mime_types[".flac"] = "audio/flac"
	mime_types[".aac"] = "audio/aac"
	mime_types[".m4a"] = "audio/aac"

	mime_types[".mp4"] = "video/mp4"
	mime_types[".webm"] = "video/webm"

	mime_types[".wgsl"] = "text/wgsl"
}

deinit_mime_types :: proc(mime_types: ^map[string]string) {
	delete(mime_types^)
}

get_mime_type :: proc(mime_types: ^map[string]string, file_path: string) -> string {
	ext := filepath.ext(file_path)
	ext_lower := strings.to_lower(ext, context.temp_allocator)

	if mime, ok := mime_types[ext_lower]; ok {
		return mime
	}

	return "application/octet-stream"
}

