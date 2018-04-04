require_relative "sql_handler"

HOST = ARGV[0] || "localhost"
PORT = ARGV[1] || 1234

system "mkdir", "-p", ServerSQL::DB_DIR

unless Dir.exist? ServerSQL::DB_DIR
  puts "No se ha podido crear el directorio #{ServerSQL::DB_DIR}"
  exit!
end

server = TCPServer.new HOST, PORT
puts "Servicio iniciado en #{HOST}:#{PORT}"
puts "Esperando clientes..."

handler = ServerSQL.new

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


# def tables(where_clause)
#   f = []
#   if where_clause.respond_to? :left
#     left_part = where_clause.left
#     right_part = where_clause.right
#     f << tables(left_part)
#     if left_part.respond_to? :name
#       f << left_part.name
#     elsif right_part.respond_to? :value
#       val = right_part.value
#       if val.respond_to?(:left) && val.left.respond_to?(:name)
#         f << where_clause.right.value.left.name
#       end
#     end
#     f << tables(right_part)
#   end
# end
