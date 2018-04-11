require "socket"
require "json"
require "ddl_parser"
require "sql-parser"
# require "ap"

class Hash
  def select_keys(*args)
    select { |k,v| args.include?(k) }
  end
end

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
      when parsed.is_a?(base::DirectSelect)
        return select_from parsed
      else
        return "Aun no soportado :("
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

        # Checar que tipo de operacion se realizo
        case parsed.statement_type
        when :create_table
          return create_table parsed
        else
          return "Aun no soportado :("
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
          when query.start_with?("delete")
            query.gsub!("delete", "select *")
            parsed = parser.scan_str query
            return delete_from parsed
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
    database_name ? "#{DB_DIR}/#{database_name}" : "#{DB_DIR}/#{@current_db}"
  end

  def table_path(table_name)
    "#{database_path}/#{table_name}.json"
  end

  def table_exists?(table_name)
    File.exist? table_path table_name
  end

  def database_exists?(database_name)
    Dir.exist? database_path database_name
  end

  def show_databases
    databases = Dir.entries(DB_DIR) - [".", ".."]
    if databases.empty?
      return "No hay ninguna base"
    end

    # Los caracteres "\0" seran reemplazados por
    # saltos de linea "\n" del lado del cliente
    "Bases existentes:\0\0#{databases.join('\0')}\0"
  end

  def show_tables
    unless @current_db
      return "Selecciona primero una base de datos: 'USE <NOMBRE_BASE>'"
    end

    tables = Dir.entries(database_path) - [".", ".."]
    if tables.empty?
      return "No hay ninguna tabla"
    end

    # Los caracteres "\0" seran reemplazados por
    # saltos de linea "\n" del lado del cliente
    "Tablas en `#{@current_db}`:\0\0#{tables.join('\0').gsub(".json", "")}\0"
  end

  def create_database(database_name)
    if database_exists? database_name
      return "Ya existe la base de datos `#{database_name}`"
    end

    Dir.mkdir database_path database_name

    "Se ha creado la base `#{database_name}`"
  end

  def drop_database(database_name)
    unless database_exists? database_name
      return "No existe la base `#{database_name}`"
    end

    system "rm", "-rf", database_path(database_name)

    "Se ha eliminado la base `#{database_name}`"
  end

  def save_table(table)
    IO.write table_path(table["meta"]["name"]), JSON.pretty_generate(table)
  end

  def create_table(parse_result)
    unless @current_db
      return "Selecciona primero una base de datos: 'USE <NOMBRE_BASE>'"
    end

    table_name = parse_result.parse_tree[:table_name].to_s
    if table_exists? table_name
      return "Ya existe la tabla `#{@current_db}`.`#{table_name}`"
    end

    table = {
      "meta" => {
        "name" => table_name,
        "statement" => parse_result.instance_variable_get("@statement").strip
      },
      "rows" => []
    }

    fields = parse_result.parse_tree[:elements]
    if fields.is_a? Hash
      # Crear tabla con un solo campo
      res = {}
      res[fields[:column][:field]] = fields[:column]
      res[fields[:column][:field]].delete :field
      table["meta"]["columns"] = [res]
    else
      table["meta"]["columns"] = fields.map do |column|
        # column = column.sort.to_h
        res = {}
        res[column[:column][:field]] = column[:column]
        res[column[:column][:field]].delete :field
        res
      end.sort_by { |field| field.keys.first.to_s }
    end

    save_table table

    "Se ha creado la tabla `#{@current_db}`.`#{table_name}`"
  end

  def drop_table(table_name)
    unless @current_db
      return "Selecciona primero una base de datos: 'USE <NOMBRE_BASE>'"
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
    table["meta"]["columns"].flat_map(&:keys)
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
      return "Selecciona primero una base de datos: 'USE <NOMBRE_BASE>'"
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

    table = get_table table_name
    column_names = column_names(table)
    row = nil

    if column_names.size != values.size
      # El tamaño de los campos de la tabla no es igual
      # a la cantidad de valores a insertar
      return "La longitud de la lista de columnas(#{column_names.size}) no coincide con la de la lista de valores(#{values.size})"
    elsif column_list = parse_result.column_list
      # Si hay una lista de atributos en la sentencia
      if (list_names = column_list.columns).is_a? Array
        # Mas de un atributo
        list_names.map! &:name
      else
        # Solamente un atributo en la lista
        list_names = [list_names.name]
      end

      if values.size != list_names.size
        # El tamaño de la lista de atributos es no es igual
        # a la cantidad de valores a insertar
        return "La longitud de la lista de columnas(#{list_names.size}) no coincide con la de la lista de valores(#{values.size})"
      end

      fields_exist = columns_exist?(table, list_names)
      unless fields_exist.first
        # Si no todas las columnas existen en la tabla
        return "No existe la columna `#{fields_exist.last}` en la tabla `#{table_name}`"
      end

      # Se crea el nuevo registro
      row = Hash[list_names.sort!.zip(values)]
    else
      # Aqui no existe una lista de columnas y la cantidad de valores a insertar
      # coincide con la lista de columnas en la tabla

      # Se crea el nuevo registro
      row = Hash[column_names.zip(values)]
    end
    # Se agrego el registro a la tabla
    table["rows"] << row
    save_table table

    "Se agrego el registro a la tabla"
  end

  def get_where_columns(where_clause)
    f = []
    if where_clause.respond_to? :left
      left_part = where_clause.left
      right_part = where_clause.right
      f << get_where_columns(left_part)
      if left_part.respond_to? :name
        f << left_part.name
      elsif right_part.respond_to? :value
        val = right_part.value
        if val.respond_to?(:left) && val.left.respond_to?(:name)
          f << where_clause.right.value.left.name
        end
      end
      f << get_where_columns(right_part)
    elsif where_clause.respond_to? :value
      get_where_columns(where_clause.value)
    end
  end

  def columns_from_where(where_clause)
    get_where_columns(where_clause).flatten.compact
  end

  def make_conditions(condition_str, columns, var_name = "col")
    columns.each do |col|
      condition_str.gsub!("`#{col}`", "#{var_name}['#{col}']")
    end

    condition_str.gsub(" AND ", " && ").gsub(" OR ", " || ").gsub(" = ", " == ").gsub(" <> ", " != ")
  end

  def delete_from(parse_result)
    unless @current_db
      return "Selecciona primero una base de datos: 'USE <NOMBRE_BASE>'"
    end

    table_name = parse_result.query_expression.table_expression.from_clause.tables.first.name
    unless table_exists? table_name
      return "No existe la tabla `#{@current_db}`.`#{table_name}`"
    end

    # Aqui la tabla existe y hay una base de datos seleccionada

    table = get_table table_name
    columns = column_names table
    rows = table["rows"]
    rows_before = rows.size
    new_rows = []

    where_clause = parse_result.query_expression.table_expression.where_clause
    if where_clause
      # Existe una sentencia WHERE
      columns_in_where = columns_from_where where_clause.search_condition
      fields_exist = columns_exist?(table, columns_in_where)
      unless fields_exist.first
        # Si no todas las columnas existen en la tabla
        return "No existe la columna `#{fields_exist.last}` en la tabla `#{table_name}`"
      end

      # Las columnas en la clausula WHERE existen en la tabla

      # Cambiar las condiciones para las columnas
      conditions = make_conditions where_clause.search_condition.to_sql, columns_in_where

      new_rows = rows.reject do |col|
        eval conditions
      end
    end

    table["rows"] = new_rows
    save_table table

    "Se eliminaron #{rows_before - new_rows.size} registros"
  end

  def format_rows(rows)
    if rows.size.zero?
      "No hay resultados"
    else
      result = rows.map { |row| row.to_s[1..-2] }.join " \0"
      "#{result}\0\0#{rows.size} resultados\0"
    end
  end

  def select_from(parse_result)
    unless @current_db
      return "Selecciona primero una base de datos: 'USE <NOMBRE_BASE>'"
    end

    table_name = parse_result.query_expression.table_expression.from_clause.tables.first.name
    unless table_exists? table_name
      return "No existe la tabla `#{@current_db}`.`#{table_name}`"
    end

    table = get_table table_name
    rows = table["rows"]
    list_names = nil

    op_type = parse_result.query_expression.list
    case
    when op_type.is_a?(SQLParser::Statement::All)
      # "SELECT *"
      list_names = column_names(table)
    else
      list_names = op_type.columns.map! &:name

      fields_exist = columns_exist?(table, list_names)
      unless fields_exist.first
        # Si no todas las columnas existen en la tabla
        return "No existe la columna `#{fields_exist.last}` en la tabla `#{table_name}`"
      end
    end

    where_clause = parse_result.query_expression.table_expression.where_clause
    if where_clause
      # Existe una sentencia WHERE
      columns_in_where = columns_from_where where_clause.search_condition
      fields_exist = columns_exist?(table, columns_in_where)
      unless fields_exist.first
        # Si no todas las columnas existen en la tabla
        return "No existe la columna `#{fields_exist.last}` en la tabla `#{table_name}`"
      end

      # Las columnas en la clausula WHERE existen en la tabla

      # Cambiar las condiciones para las columnas
      conditions = make_conditions where_clause.search_condition.to_sql, columns_in_where

      rows.select! do |col|
        eval conditions
      end
    end

    rows.map! { |row| row.select_keys(*list_names) }

    format_rows rows
  end
end

# numbers = [*1..9999]
#
# 50.times do
#   puts "insert into tabla_2 values (#{numbers.sample}, '#{('a'..'z').to_a.shuffle[0,8].join}');"
# end
# delete from tabla_2 where columna_1 > 100 and columna_1 < 500;
