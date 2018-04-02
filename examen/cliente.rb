require "socket"

HOST = "localhost"
PORT = 4567

def get_query
  # Variable para acumular consultas
  # Contendra cada una de las consultas hasta que
  # se introduzca un ';' para finalizar
  query_acc = []
  query = ""

  loop do
    query = gets.strip.downcase

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

loop do
  query = get_query
  socket.puts query
  puts socket.gets
end
