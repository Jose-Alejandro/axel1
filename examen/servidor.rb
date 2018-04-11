require_relative "sql_handler"

HOST = ARGV[0] || "localhost"
PORT = ARGV[1] || 1234

system "mkdir", "-p", SQLHandler::DB_DIR

unless Dir.exist? SQLHandler::DB_DIR
  puts "No se ha podido crear el directorio #{SQLHandler::DB_DIR}"
  exit!
end

server = TCPServer.new HOST, PORT
puts "Servicio iniciado en #{HOST}:#{PORT}"
puts "Esperando clientes..."

handler = SQLHandler.new

loop do
  Thread.start(server.accept) do |client|
    cl_addr = client.remote_address
    puts "Cliente conectado desde #{cl_addr.ip_address}:#{cl_addr.ip_port}"
    while request = client.gets
      puts "[#{cl_addr.ip_address}:#{cl_addr.ip_port}]"
      puts "Solicitado: #{request}"
      response = handler.handle request

      client.puts response

      puts "Respuesta:"
      if response[-1] == "\0"
        puts response.gsub("\0", "\n")
      else
        puts response
        puts
      end
    end
  end
end
