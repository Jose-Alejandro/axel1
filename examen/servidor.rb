require "socket"
require "json"
require "ddl_parser"
require "sql-parser"
require "ap"

DB_DIR = "databases"
$current_db = "test"

def handle_query(query)
  parser = SQLParser::Parser.new
  parsed = nil
  response = "response"

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
      return ddl_error.message
    end

    if parsed.parse_error
      return parsed.parse_error.message
    end

    case parsed.parse_tree[:operation].to_s
    when "create table"
      puts "create"
      # content = {meta:{table_name: parser.parse_tree[:table_name]}}
      ap parsed.parse_tree[:elements]
      content[:meta][:columns] = parsed.parse_tree[:elements].map do |column|
        res = {}
        res[column[:column][:field]] = column[:column]
        res[column[:column][:field]].delete :field
        res
      end

      table_path = "#{DB_DIR}/#{$current_db}/#{content[:meta][:table_name]}.json"

      IO.write table_path, JSON.pretty_generate(content)
      puts "klfjkjasklfjklsdjfkdsjkfl"
      response = "Tabla #{content[:meta][:table_name]} creada con exito"
    end
  end

  response
end

server = TCPServer.new 4567

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
