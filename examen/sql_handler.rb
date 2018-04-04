require "socket"
require "json"
require "ddl_parser"
require "sql-parser"
# require "ap"

class SQLHandler
  DB_DIR = "databases"

  def initialize
    @query = @current_db = nil
  end

  def handle(query)
    parser = SQLParser::Parser.new
    parsed = nil

    begin
      # Probar si es tipo DML
      parsed = parser.scan_str query

      # SQLParser::Parser levantara una excepcion
      # del tipo Racc::ParseError en caso de que la
      # sentencia no sea de tipo DML
      # La excepcion es manejada abajo

      # Aqui la consulta es de tipo DML

      base = SQLParser::Statement

      case
      when parsed.is_a?(base::Insert)
        return insert_into parsed
        # table_name = parsed.table_reference.name
        # values =
        # TODO: metodo para checar atributos existan en tabla **
        # TODO: metodo para insertar en tabla
        # checar la base actual **
        # checar que la tabla exista **
        # checar que los atributos existan
        # checar el tamano de los valores
        # checar los default
        # checar que los tamanos coincidan


        # return table_name
      else
        return "no soportado"
      end

    rescue Racc::ParseError => dml_error
      # La sentencia no es de tipo DML
      begin
        # Probar si es tipo DDL
        parsed = DDLParser::Parser.new query

        # DDLParser::Parser levantara una excepcion
        # del tipo RuntimeError en caso de que la
        # sentencia no sea de tipo DDL
        # La excepcion es manejada abajo

        # Aqui la consulta es de tipo DDL

        # Si la sintaxis es correcta pero existe algun error
        if parsed.parse_error
          return parsed.parse_error.message
        end

        tree = parsed.parse_tree

        # Checar que tipo de operacion se realizo
        case tree[:operation].to_s
        when "create table"
          return create_table tree[:table_name], tree[:elements]
        end
      rescue RuntimeError => ddl_error
        # La sentencia no es de tipo DDL
        # Probar otros tipos
        begin
          case
          when query.start_with?("create database")
            db_name = query.split[2]

            return create_database db_name
          when query.start_with?("drop database")
            db_name = query.split[2]

            return drop_database db_name
          when query.start_with?("drop table")
            table_name = query.split[2]

            return drop_table table_name
          when query.start_with?("use")
            db_name = query.split[1]

            return use_database db_name
          when query.start_with?("show databases")
            return show_databases
          when query.start_with?("show tables")
            return show_tables
          end

        rescue => error
          # Se produjo alguna excepcion
          return error.message
        end

        # No es ninguno de los tipos anteriores
        # Retornar mensaje de la excepcion
        # que arroja el parser
        return ddl_error.message
      rescue => error
        # Se produjo alguna excepcion
        return error.message
      end

    rescue => error
      # Se produjo alguna excepcion
      return error.message
    end
  end

  private

  def database_path(database_name = nil)
    if database_name
      "#{DB_DIR}/#{database_name}"
    end

    "#{DB_DIR}/#{@current_db}"
  end

  def table_path(table_name)
    "#{database_path}/#{table_name}.json"
  end

  def table_exists?(table_name)
    File.exist? table_path(table_name)
  end

  def database_exists?(database_name)
    Dir.exist? database_path(database_name)
  end

  def show_databases
    databases = Dir.entries(DB_DIR) - [".", ".."]
    if databases.empty?
      return "No hay ninguna base"
    end

    # Los caracteres "\0" seran reemplazados por
    # saltos de linea "\n" del lado del cliente
    "Bases existentes:\0\0#{databases.join("\0")}\0"
  end

  def show_tables
    unless @current_db
      return "Selecciona primero una base de datos: 'USE <NOMBRE_TABLA>'"
    end

    tables = Dir.entries(database_path) - [".", ".."]
    if tables.empty?
      return "No hay ninguna tabla"
    end

    # Los caracteres "\0" seran reemplazados por
    # saltos de linea "\n" del lado del cliente
    "Tablas en `#{@current_db}`:\0\0#{tables.join("\0").gsub(".json", "")}\0"
  end

  def create_database(database_name)
    if database_exists? database_name
      return "Ya existe la base de datos `#{database_name}`"
    end

    Dir.mkdir database_path(database_name)

    "Se ha creado la base `#{database_name}`"
  end

  def drop_database(database_name)
    unless database_exists? database_name
      return "No existe la base `#{database_name}`"
    end

    system "rm", "-rf", database_path(database_name)

    "Se ha eliminado la base `#{database_name}`"
  end

  def create_table(table_name, fields)
    unless @current_db
      return "Selecciona primero una base de datos: 'USE <NOMBRE_TABLA>'"
    end

    if table_exists? table_name
      return "Ya existe la tabla `#{@current_db}`.`#{table_name}`"
    end

    content = {meta:{table_name: table_name}}

    if fields.is_a? Hash
      # Crear tabla con un solo campo
      res = {}
      res[fields[:column][:field]] = fields[:column]
      res[fields[:column][:field]].delete :field
      content[:meta][:columns] = [res]
    else
      content[:meta][:columns] = fields.map do |column|
        res = {}
        res[column[:column][:field]] = column[:column]
        res[column[:column][:field]].delete :field
        res
      end
    end

    IO.write table_path(table_name), JSON.pretty_generate(content)

    "Se ha creado la tabla `#{@current_db}`.`#{table_name}`"
  end

  def drop_table(table_name)
    unless @current_db
      return "Selecciona primero una base de datos: 'USE <NOMBRE_TABLA>'"
    end

    unless table_exists? table_name
      return "No existe la tabla `#{@current_db}`.`#{table_name}`"
    end

    File.delete table_path(table_name)

    "Se ha eliminado la base `#{@current_db}`.`#{table_name}`"
  end

  def use_database(database_name)
    unless database_exists? database_name
      return "No existe la base `#{database_name}`"
    end

    @current_db = database_name

    "Ahora usando `#{database_name}`"
  end

  def get_table(table_name)
    JSON.parse IO.read table_path table_name
  end

  def column_names(table)
    table["meta"]["columns"].map(&:keys)
  end

  def columns_exist?(table, columns)
    table_columns = column_names(table)

    columns.each do |column|
      unless table_columns.include? column
        return false, column
      end
    end

    [true, nil]
  end

  def insert_into(parse_result)
    unless @current_db
      return "Selecciona primero una base de datos: 'USE <NOMBRE_TABLA>'"
    end

    table_name = parse_result.table_reference.name
    unless table_exists? table_name
      return "No existe la tabla `#{@current_db}`.`#{table_name}`"
    end

    # Aqui la tabla existe y hay una base de datos seleccionada

    if (values = parse_result.in_value_list.values).is_a? Array
      # Mas de un valor a insertar
      values.map! &:value
    else
      # Un solo valor para insertar
      values = [values.value]
    end

    if column_list = parse_result.column_list
      # Si hay una lista de atributos en la sentencia
      if (list_names = column_list.columns).is_a? Array
        # Mas de un atributo
        list_names.map! &:name
      else
        # Solamente un atributo en la lista
        list_names = [list_names.name]
      end

      unless values.size == list_names.size
        return "La longitud de la lista de columnas(#{list_names.size}) no coincide con la de la lista de valores(#{values.size})"
      end

      fields_exist = columns_exist?(table_name, list_names)
      unless fields_exist.first
        return "No existe la columna `#{fields_exist.last}` en la tabla `#{table_name}`"
      end
    end
    puts list_names, values

    "Todo ok xd"
  end
end
