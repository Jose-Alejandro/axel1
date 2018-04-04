require "socket"

HOST = ARGV[0] || "localhost"
PORT = ARGV[1] || 1234
PROMPT = "MySQL-v.axel> "

def get_query
  # Variable para acumular consultas
  # Contendra cada una de las consultas hasta que
  # se introduzca un ';' para finalizar
  query_acc = []
  query = ""

  loop do
    print PROMPT
    query = STDIN.gets.strip.downcase

    if query.start_with? "exit"
      # Salir del programa
      puts "Bye"
      exit!
    end

    if query[-1] == ';'
      # Si la sentencia SQL se ha terminado
      query_acc << query[0..-2]
      query = query_acc.join ' '
      break
    else
      # Seguir acumulando las consultas
      query_acc << query
      next
    end
  end

  query
end

socket = TCPSocket.new HOST, PORT
addr = socket.remote_address
puts "Conectado a #{addr.ip_address}:#{addr.ip_port}"
puts

loop do
  query = get_query
  socket.puts query

  response = socket.gets
  if response[-2] == "\0"
    puts response.gsub("\0", "\n")
  else
    puts response
    puts
  end
end
