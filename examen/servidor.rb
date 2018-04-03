require "socket"
require "json"
require 'ddl_parser'
require 'sql-parser'
#require 'ap'

DB_DIR = "databases"
HOST = "localhost"
PORT = 1234
$current_db = "test"
#host="192.127.4.123"

def table_exists?(table)
  File.exist? "#{DB_DIR}/#{$current_db}/#{table}.json"
end

def database_exists?(database)
  Dir.exist? "#{DB_DIR}/#{database}"
end

def handle_query(query)
  parser = SQLParser::Parser.new
  parsed = nil
  response = ""

  begin
    # Probar si es tipo DML
    parsed = parser.scan_str query
  rescue Racc::ParseError => dml_error
    # La sentencia no es de tipo DML
    begin
      # Probar si es tipo DDL
      parsed = DDLParser::Parser.new query
    rescue RuntimeError => ddl_error
      # La sentencia no es de tipo DDL
      # Probar otros tipos
      case
      when query.start_with?("create database")
        db_name = query.split[2]

        if database_exists?(db_name)
          return "La base de datos `#{db_name}` ya existe"
        end

        Dir.mkdir "#{DB_DIR}/#{db_name}"
        return "Se ha creado la base `#{db_name}`"
      when query.start_with?("drop database")
        db_name = query.split[2]

        unless database_exists?(db_name)
          return "La base de datos `#{db_name}` no existe"
        end

        system "rm", "-rf", "#{DB_DIR}/#{db_name}"
        return "Se ha eliminado la base `#{db_name}`"
      when query.start_with?("drop table")
        table_name = query.split[2]

        unless $current_db
          return "Selecciona una primero una base de datos: USE [NOMBRE_TABLA]"
        end

        unless table_exists?(table_name)
          return "La tabla `#{db_name}` no existe"
        end

        table_path = "#{DB_DIR}/#{$current_db}/#{table_name}.json"

        File.delete table_path
        return "Se ha eliminado la base `#{$current_db}`.`#{db_name}`"
      when query.start_with?("use")
        db_name = query.split[1]

        unless database_exists?(db_name)
          return "La base de datos `#{db_name}` no existe"
        end

        $current_db = db_name
        return "Ahora usando `#{db_name}`"
      end
      return ddl_error.message
    end

    if parsed.parse_error
      return parsed.parse_error.message
    end

    tree = parsed.parse_tree

    case tree[:operation].to_s
    when "create table"
      unless $current_db
        return "Selecciona una primero una base de datos: USE [NOMBRE_TABLA]"
      end

      table_name = tree[:table_name]
      if table_exists? table_name
        return "La tabla #{table_name} ya existe"
      end

      content = {meta:{table_name: table_name}}
      content[:meta][:columns] = tree[:elements].map do |column|
        res = {}
        res[column[:column][:field]] = column[:column]
        res[column[:column][:field]].delete :field
        res
        end

      table_path = "#{DB_DIR}/#{$current_db}/#{table_name}.json"

      IO.write table_path, JSON.pretty_generate(content)
      return "Se ha creado la base `#{$current_db}`.`#{db_name}`"
    end
  end

  response
end

system "mkdir", "-p", DB_DIR

unless Dir.exist? DB_DIR
  puts "No se ha podido crear el directorio #{DB_DIR}"
  exit!
end

server = TCPServer.new HOST, PORT
puts "Servicio iniciado en #{HOST}:#{PORT}"
puts "Esperando clientes..."

loop do
  Thread.start(server.accept) do |client|
    cl_addr = client.remote_address
    puts "Cliente conectado desde #{cl_addr.ip_address}:#{cl_addr.ip_port}"
    while query = client.gets
      puts query
      client.puts handle_query query
    end
  end
end


def tables(where_clause)
  f = []
  if where_clause.respond_to? :left
    f << tables(where_clause.left)
    if where_clause.left.respond_to? :name
      f << where_clause.left.name
    end
    f << tables(where_clause.right)
  end
end
