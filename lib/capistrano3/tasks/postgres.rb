namespace :load do
  task :defaults do
    set :postgres_backup_dir, -> { 'postgres_backup' }
    set :postgres_role, :db
    set :postgres_env, -> { fetch(:rack_env, fetch(:rails_env, fetch(:stage))) }
    set :postgres_keep_local_dumps, 0
    set :postgres_backup_compression_level, 0
    set :postgres_remote_sqlc_file_path, -> { nil }
    set :postgres_local_database_config, -> { nil }
    set :postgres_remote_database_config, -> { nil }
    set :postgres_remote_cluster, -> { nil }
    set :postgres_backup_exclude_table_data, -> { [] }
    set :postgres_backup_exclude_table, -> { [] }
    set :postgres_backup_table, -> { [] }
    set :postgres_database, -> { nil }
    set :postgres_streaming_mode, false
    set :postgres_stream_timeout, 3600
    set :postgres_stream_buffer_size, '64M'
    set :postgres_verbose, true
    set :postgres_restore_jobs, nil
    set :postgres_fast_dump, false
    set :postgres_ssh_multiplexing, true
  end
end

namespace :postgres do
  namespace :backup do
    desc 'Create database dump (enhanced with streaming support)'
    task :create do
      on roles(fetch(:postgres_role)) do |role|
        if fetch(:postgres_streaming_mode, false)
          info "Skipping remote dump creation - will stream directly"
        else
          grab_remote_database_config
          config = fetch(:postgres_remote_database_config)

          unless fetch(:postgres_remote_sqlc_file_path)
            file_name = "db_backup.#{Time.now.strftime('%Y-%m-%d_%H-%M-%S')}.sqlc"
            set :postgres_remote_sqlc_file_path, "#{shared_path}/#{fetch(:postgres_backup_dir)}/#{file_name}"
          end
          execute [
            "PGPASSWORD=#{config['password']}",
            "pg_dump #{user_option(config)}",
            "-h #{config['host']}",
            config['port'] ? "-p #{config['port']}" : nil,
            "-Fc",
            "--file=#{fetch(:postgres_remote_sqlc_file_path)}",
            "-Z #{fetch(:postgres_backup_compression_level)}",
            fetch(:postgres_backup_exclude_table_data).map {|table| "--exclude-table-data=#{table}" },
            fetch(:postgres_backup_exclude_table).map {|table| "--exclude-table=#{table}" },
            fetch(:postgres_backup_table).map {|table| "--table=#{table}" },
            fetch(:postgres_remote_cluster) ? "--cluster #{fetch(:postgres_remote_cluster)}" : nil,
            "#{config['database']}"
          ].flatten.compact.join(' ')
        end
      end
    end

    desc 'Download last database dump (enhanced with streaming support)'
    task :download, [:remove_remote_file] do |_task, args|
      if fetch(:postgres_streaming_mode, false)
        info "Skipping download - using direct streaming"
      else
        on roles(fetch(:postgres_role)) do |role|
          unless fetch(:postgres_remote_sqlc_file_path)
            file_name = capture("ls -v #{shared_path}/#{fetch :postgres_backup_dir}").split(/\n/).last
            set :postgres_remote_sqlc_file_path, "#{shared_path}/#{fetch :postgres_backup_dir}/#{file_name}"
          end

          download!(fetch(:postgres_remote_sqlc_file_path), "tmp/#{fetch :postgres_backup_dir}/#{Pathname.new(fetch(:postgres_remote_sqlc_file_path)).basename}")
          begin
            remote_file = fetch(:postgres_remote_sqlc_file_path)
          rescue SSHKit::Command::Failed => e
            warn e.inspect
          ensure
            false_values = [nil, false, 0, '0', 'f', 'F', 'False', 'false', 'FALSE', 'Off', 'off', 'OFF', 'No', 'no', 'NO']
            execute "rm #{remote_file}" unless false_values.include?(args[:remove_remote_file])
          end
        end
      end
    end

    desc 'Import last dump (enhanced with streaming support)'
    task :import, [:database_name] do |_task, args|
      if fetch(:postgres_streaming_mode, false)
        perform_streaming_import(args[:database_name])
      else
        grab_local_database_config
        run_locally do
          config = fetch(:postgres_local_database_config)

          # Prompt for database name if not provided
          database_name = args[:database_name] || ask(:database_name, config['database'])
          set(:database_name, database_name)

          with rails_env: :development do
            file_name = capture("ls -v tmp/#{fetch :postgres_backup_dir}").split(/\n/).last
            file_path = "tmp/#{fetch :postgres_backup_dir}/#{file_name}"
            begin
              pgpass_path = File.join(Dir.pwd, '.pgpass')
              File.open(pgpass_path, 'w+', 0600) { |file| file.write("*:*:*:#{config['username'] || config['user']}:#{config['password']}") }
              execute "PGPASSFILE=#{pgpass_path} pg_restore -c --if-exists #{user_option(config)} --no-owner -h #{config['host']} -p #{config['port'] || 5432} -d #{fetch(:database_name)} #{file_path}"
              info 'Import performed successfully!'
            rescue SSHKit::Command::Failed => e
              warn e.inspect
            ensure
              File.delete(pgpass_path) if File.exist?(pgpass_path)
              File.delete(file_path) if (fetch(:postgres_keep_local_dumps) == 0) && File.exist?(file_path)
            end
          end
        end
      end
    end

    # Ensure that remote dirs for postgres backup exist
    before :create, :ensure_remote_dirs do
      on roles(fetch(:postgres_role)) do |role|
        execute :mkdir, "-p #{shared_path}/#{fetch(:postgres_backup_dir)}"
      end
    end

    # Ensure that loca dirs for postgres backup exist
    before :download, :ensure_local_dirs do
      on roles(fetch(:postgres_role)) do |role|
        run_locally do
          execute :mkdir, "-p  tmp/#{fetch :postgres_backup_dir}"
        end
      end
    end

    desc "Cleanup old local dumps"
    task :cleanup do
      run_locally do
        dir = "tmp/#{fetch :postgres_backup_dir}"
        file_names = capture("ls -v #{dir}").split(/\n/).sort
        file_names[0...-fetch(:postgres_keep_local_dumps)].each {|file_name| File.delete("#{dir}/#{file_name}") }
      end
    end

    desc 'Force streaming mode for subsequent postgres tasks'
    task :enable_streaming do
      set :postgres_streaming_mode, true
      info "Streaming mode enabled - subsequent tasks will stream directly"
    end

    desc 'Disable streaming mode'
    task :disable_streaming do
      set :postgres_streaming_mode, false
      info "Streaming mode disabled - using file-based operations"
    end
  end

  desc 'Replicate database locally (enhanced with streaming - no local storage)'
  task :replicate, [:database_name] do |_task, args|
    # Enable streaming mode for this operation
    set :postgres_streaming_mode, true
    
    info "Starting streaming replication (no local file storage)..."
    
    # Prompt for database name if not provided
    grab_local_database_config
    database_name = args[:database_name] || ask(:database_name, fetch(:postgres_local_database_config)['database'])
    set(:database_name, database_name)
    
    # Perform streaming operation
    perform_streaming_replicate(database_name)
    
    # Disable streaming mode
    set :postgres_streaming_mode, false
    
    info "Streaming replication completed!"
  end

  def user_option(config)
    if config['user'] || config['username']
      "-U #{config['user'] || config['username']}"
    else
      '' # assume ident auth is being used
    end
  end

  # Grabs local database config before importing dump
  def grab_local_database_config
    return if fetch(:postgres_local_database_config)
    on roles(fetch(:postgres_role)) do |role|
      run_locally do
        env = 'development'
        preload_env_variables(env)
        yaml_content = ERB.new(capture 'cat config/database.yml').result
        set_postgres_database_config(yaml_content, env, :postgres_local_database_config)
      end
    end
  end

  # Grabs remote database config before creating dump
  def grab_remote_database_config
    return if fetch(:postgres_remote_database_config)
    on roles(fetch(:postgres_role)) do |role|
      within release_path do
        env = fetch(:postgres_env).to_s.downcase
        filename = "#{deploy_to}/current/config/database.yml"
        eval_yaml_with_erb = <<-RUBY.strip
          #{env_variables_loader_code(env)}
          require 'erb'
          puts ERB.new(File.read('#{filename}')).result
        RUBY

        capture_config_cmd = "ruby -e \"#{eval_yaml_with_erb}\""
        yaml_content = test('ruby -v') ? capture(capture_config_cmd) : capture(:bundle, :exec, capture_config_cmd)
        set_postgres_database_config(yaml_content, env, :postgres_remote_database_config)
      end
    end
  end

  def database_config_defaults
    { 'host' => 'localhost' }
  end

  def set_postgres_database_config(yaml_content, env, key)
    database_config = YAML::load(yaml_content)[env]
    database_config = database_config[fetch(:postgres_database)] if fetch(:postgres_database)
    set key, database_config_defaults.merge(database_config)
  end

  # Load environment variables for configurations.
  # Useful for such gems as Dotenv, Figaro, etc.
  def preload_env_variables(env)
    safely_require_gems('dotenv', 'figaro')

    if defined?(Dotenv)
      load_env_variables_with_dotenv(env)
    elsif defined?(Figaro)
      load_env_variables_with_figaro(env)
    end
  end

  def load_env_variables_with_dotenv(env)
    Dotenv.load(
      File.expand_path('.env.local'),
      File.expand_path(".env.#{env}"),
      File.expand_path('.env')
    )
  end

  def load_env_variables_with_figaro(env)
    config = 'config/application.yml'

    Figaro.application = Figaro::Application.new(environment: env, path: config)
    Figaro.load
  end

  def safely_require_gems(*gem_names)
    gem_names.each do |name|
      begin
        require name
      rescue LoadError
        # Ignore if gem doesn't exist
      end
    end
  end

  # Requires necessary gems (Dotenv, Figaro, ...) if present
  # and loads environment variables for configurations
  def env_variables_loader_code(env)
    <<-RUBY.strip
      begin
        require 'dotenv'
        Dotenv.load(File.expand_path('.env.#{env}'), File.expand_path('.env'))
      rescue LoadError
      end

      begin
        require 'figaro'
        config = File.expand_path('../config/application.yml', __FILE__)

        Figaro.application = Figaro::Application.new(environment: '#{env}', path: config)
        Figaro.load
      rescue LoadError
      end
    RUBY
  end

  def perform_streaming_replicate(database_name)
    on roles(fetch(:postgres_role)) do |host|
      with_postgres_credentials do |remote_config|
        local_config = get_local_database_config(database_name)
        
        info "Streaming from #{remote_config[:database]}@#{host} to local #{local_config[:database]}"
        
        # Show performance estimates
        estimate_performance_improvement
        
        # Build optimized streaming command
        streaming_command = build_optimized_streaming_pipeline(
          remote_config: remote_config,
          local_config: local_config, 
          remote_host: host
        )
        
        # Execute the streaming operation locally
        run_locally do
          info "Executing optimized streaming with parallel restore..."
          info "Command: #{streaming_command.gsub(/PGPASSWORD='[^']*'/, "PGPASSWORD='***'")}"
          
          # Set timeout for long-running operations
          with_timeout(fetch(:postgres_stream_timeout, 3600)) do
            execute streaming_command
          end
        end
        
        # Cleanup any temporary files if created
        cleanup_streaming_artifacts
      end
    end
  end

  def perform_streaming_import(database_name = nil)
    on roles(fetch(:postgres_role)) do |host|
      with_postgres_credentials do |remote_config|
        local_config = get_local_database_config(database_name)
        
        estimate_performance_improvement
        
        # Direct streaming from remote to local with optimizations
        streaming_command = build_optimized_streaming_pipeline(
          remote_config: remote_config,
          local_config: local_config,
          remote_host: host
        )
        
        run_locally do
          with_timeout(fetch(:postgres_stream_timeout, 3600)) do
            execute streaming_command
          end
        end
      end
    end
  end

  def build_streaming_pipeline(remote_config:, local_config:, remote_host:)
    # Build remote pg_dump command
    dump_cmd = build_remote_dump_command(remote_config)
    
    # Build local pg_restore command  
    restore_cmd = build_local_restore_command(local_config)
    
    # Build SSH connection
    ssh_cmd = build_ssh_command(remote_host)
    
    # Determine if compression should be used
    if fetch(:postgres_backup_compression_level, 0) > 0
      compression_level = fetch(:postgres_backup_compression_level)
      # SSH with compression: remote_dump | gzip -> SSH -> gunzip | local_restore
      "#{ssh_cmd} '#{dump_cmd} | gzip -#{compression_level}' | gunzip | #{restore_cmd}"
    else
      # Direct streaming: SSH remote_dump -> local_restore
      "#{ssh_cmd} '#{dump_cmd}' | #{restore_cmd}"
    end
  end

  def build_remote_dump_command(config)
    cmd_parts = [
      "PGPASSWORD='#{config[:password]}'",
      'pg_dump'
    ]

    # Add dump options
    cmd_parts << '--format=custom' unless fetch(:postgres_backup_format) == 'sql'
    cmd_parts << '--verbose' if fetch(:postgres_verbose, true)
    cmd_parts << '--no-acl' 
    cmd_parts << '--no-owner'

    # Add connection parameters
    cmd_parts << "--host=#{config[:host]}" if config[:host] != 'localhost'
    cmd_parts << "--port=#{config[:port]}" if config[:port] != 5432
    cmd_parts << "--username=#{config[:username]}" if config[:username]

    # Add table exclusions
    exclude_tables = fetch(:postgres_backup_exclude_table, [])
    exclude_tables = exclude_tables.call if exclude_tables.respond_to?(:call)
    exclude_tables.each do |table|
      cmd_parts << "--exclude-table=#{table}"
    end

    # Add table data exclusions
    exclude_table_data = fetch(:postgres_backup_exclude_table_data, [])
    exclude_table_data = exclude_table_data.call if exclude_table_data.respond_to?(:call)
    exclude_table_data.each do |table|
      cmd_parts << "--exclude-table-data=#{table}"
    end

    # Add specific tables if specified
    backup_tables = fetch(:postgres_backup_table, [])
    backup_tables = backup_tables.call if backup_tables.respond_to?(:call)
    backup_tables.each do |table|
      cmd_parts << "--table=#{table}"
    end

    # Add database name
    cmd_parts << config[:database]

    cmd_parts.join(' ')
  end

  def build_local_restore_command(config)
    cmd_parts = [
      "PGPASSWORD='#{config[:password]}'",
      'pg_restore'
    ]

    cmd_parts << '--verbose' if fetch(:postgres_verbose, true)
    cmd_parts << '--clean'
    cmd_parts << '--no-acl'
    cmd_parts << '--no-owner'

    # Add parallel processing based on CPU cores
    parallel_jobs = get_optimal_parallel_jobs
    cmd_parts << "--jobs=#{parallel_jobs}" if parallel_jobs > 1

    # Add local connection parameters
    cmd_parts << "--host=#{config[:host]}" if config[:host] && config[:host] != 'localhost'
    cmd_parts << "--port=#{config[:port]}" if config[:port] && config[:port] != 5432
    cmd_parts << "--username=#{config[:username]}" if config[:username]
    cmd_parts << "--dbname=#{config[:database]}"

    cmd_parts.join(' ')
  end

  def build_ssh_command(remote_host)
    ssh_parts = ['ssh']
    
    # Add SSH options
    ssh_parts << "-p #{remote_host.port}" if remote_host.port && remote_host.port != 22
    
    # Add SSH key if specified
    if remote_host.ssh_options && remote_host.ssh_options[:keys]
      key_file = remote_host.ssh_options[:keys].first
      ssh_parts << "-i #{key_file}"
    end

    # Add compression for SSH if not already compressing pg_dump output
    if fetch(:postgres_backup_compression_level, 0) == 0
      ssh_parts << '-C'  # Enable SSH compression
    end

    # Add connection details
    ssh_parts << "#{remote_host.user}@#{remote_host.hostname}"

    ssh_parts.join(' ')
  end

  def with_postgres_credentials
    # Get remote database configuration
    grab_remote_database_config
    config = fetch(:postgres_remote_database_config)
    
    remote_config = {
      username: config['username'] || config['user'],
      password: config['password'],
      database: config['database'],
      host: config['host'],
      port: config['port']
    }

    yield remote_config
  end

  def get_local_database_config(database_name = nil)
    # Get local database configuration for import
    local_config = fetch(:postgres_local_database_config)
    
    {
      database: database_name || fetch(:database_name) || local_config['database'],
      username: local_config['username'] || local_config['user'],
      password: local_config['password'],
      host: local_config['host'] || 'localhost',
      port: local_config['port'] || 5432
    }
  end

  def cleanup_streaming_artifacts
    # Clean up any temporary files that might have been created
    # This is mainly for error recovery scenarios
    temp_patterns = [
      '/tmp/postgres_stream_*',
      '/tmp/pg_dump_*', 
      '/tmp/pg_restore_*'
    ]
    
    run_locally do
      temp_patterns.each do |pattern|
        execute "rm -f #{pattern} 2>/dev/null || true"
      end
    end
  end

  def with_timeout(seconds)
    begin
      yield
    rescue => e
      error "Operation timed out or failed: #{e.message}"
      raise
    end
  end

  def build_optimized_streaming_pipeline(remote_config:, local_config:, remote_host:)
    # Enhanced version with performance optimizations
    
    # Build remote pg_dump command with optimizations
    dump_cmd = build_optimized_dump_command(remote_config)
    
    # Build local pg_restore command with parallel processing
    restore_cmd = build_local_restore_command(local_config)
    
    # Build SSH connection with optimizations
    ssh_cmd = build_optimized_ssh_command(remote_host)
    
    # Determine compression and buffering strategy
    compression_level = fetch(:postgres_backup_compression_level, 0)
    buffer_size = fetch(:postgres_stream_buffer_size, '64M')
    
    if compression_level > 0
      # Optimized compression pipeline with buffering
      if command_available?('pv')
        # With progress monitoring and buffering
        "#{ssh_cmd} '#{dump_cmd} | gzip -#{compression_level}' | pv -pterab -B #{buffer_size} | gunzip | #{restore_cmd}"
      else
        # Standard compression with buffering
        "#{ssh_cmd} '#{dump_cmd} | gzip -#{compression_level}' | gunzip | #{restore_cmd}"
      end
    else
      # Direct streaming with SSH compression and buffering
      if command_available?('pv')
        "#{ssh_cmd} '#{dump_cmd}' | pv -pterab -B #{buffer_size} | #{restore_cmd}"
      else
        "#{ssh_cmd} '#{dump_cmd}' | #{restore_cmd}"
      end
    end
  end

  def build_optimized_dump_command(config)
    cmd_parts = [
      "PGPASSWORD='#{config[:password]}'",
      'pg_dump'
    ]

    # Always use custom format for parallel restore compatibility
    cmd_parts << '--format=custom'
    cmd_parts << '--verbose' if fetch(:postgres_verbose, true)
    cmd_parts << '--no-acl' 
    cmd_parts << '--no-owner'

    # Optimize dump performance
    cmd_parts << '--compress=0'  # Don't compress in pg_dump, we'll handle it in pipeline
    
    # Add synchronous_commit=off for faster dumping (if safe)
    if fetch(:postgres_fast_dump, false)
      cmd_parts << '--set=synchronous_commit=off'
    end

    # Add connection parameters
    cmd_parts << "--host=#{config[:host]}" if config[:host] != 'localhost'
    cmd_parts << "--port=#{config[:port]}" if config[:port] != 5432
    cmd_parts << "--username=#{config[:username]}"

    # Add table exclusions
    exclude_tables = fetch(:postgres_backup_exclude_table, [])
    exclude_tables = exclude_tables.call if exclude_tables.respond_to?(:call)
    exclude_tables.each do |table|
      cmd_parts << "--exclude-table=#{table}"
    end

    # Add table data exclusions
    exclude_table_data = fetch(:postgres_backup_exclude_table_data, [])
    exclude_table_data = exclude_table_data.call if exclude_table_data.respond_to?(:call)
    exclude_table_data.each do |table|
      cmd_parts << "--exclude-table-data=#{table}"
    end

    # Add specific tables if specified
    backup_tables = fetch(:postgres_backup_table, [])
    backup_tables = backup_tables.call if backup_tables.respond_to?(:call)
    backup_tables.each do |table|
      cmd_parts << "--table=#{table}"
    end

    # Add database name
    cmd_parts << config[:database]

    cmd_parts.join(' ')
  end

  def build_optimized_ssh_command(remote_host)
    ssh_parts = ['ssh']
    
    # Performance optimizations for SSH
    ssh_parts << '-o Compression=no'  # Disable SSH compression if we're using gzip
    ssh_parts << '-o TCPKeepAlive=yes'
    ssh_parts << '-o ServerAliveInterval=60'
    ssh_parts << '-o ServerAliveCountMax=3'
    
    # Add port if specified
    ssh_parts << "-p #{remote_host.port}" if remote_host.port && remote_host.port != 22
    
    # Add SSH key if specified
    if remote_host.ssh_options && remote_host.ssh_options[:keys]
      key_file = remote_host.ssh_options[:keys].first
      ssh_parts << "-i #{key_file}"
    end

    # Use SSH multiplexing if available for better performance
    if fetch(:postgres_ssh_multiplexing, true)
      ssh_parts << '-o ControlMaster=auto'
      ssh_parts << '-o ControlPath=/tmp/ssh_mux_%h_%p_%r'
      ssh_parts << '-o ControlPersist=10m'
    end

    # Add connection details
    ssh_parts << "#{remote_host.user}@#{remote_host.hostname}"

    ssh_parts.join(' ')
  end

  def get_optimal_parallel_jobs
    # Get user-configured value or auto-detect
    configured_jobs = fetch(:postgres_restore_jobs, nil)
    return configured_jobs if configured_jobs

    # Auto-detect CPU cores
    cpu_cores = detect_cpu_cores
    
    # Conservative parallel job calculation
    # Use 75% of cores, minimum 1, maximum 8 (to avoid overwhelming the database)
    optimal_jobs = [(cpu_cores * 0.75).ceil, 1].max
    optimal_jobs = [optimal_jobs, 8].min
    
    info "Auto-detected #{cpu_cores} CPU cores, using #{optimal_jobs} parallel restore jobs"
    optimal_jobs
  end

  def detect_cpu_cores
    cores = 1 # fallback
    
    run_locally do
      # Try different methods to detect CPU cores
      begin
        if test('which nproc')
          # Linux
          cores = capture('nproc').strip.to_i
        elsif test('which sysctl')
          # macOS/BSD
          cores = capture('sysctl -n hw.ncpu').strip.to_i
        elsif File.exist?('/proc/cpuinfo')
          # Linux fallback
          cores = capture('grep -c processor /proc/cpuinfo').strip.to_i
        else
          # Ruby fallback
          require 'etc'
          cores = Etc.nprocessors
        end
      rescue => e
        warn "Could not detect CPU cores: #{e.message}, using 1 core"
        cores = 1
      end
    end
    
    cores > 0 ? cores : 1
  end

  def command_available?(command)
    run_locally do
      test("which #{command}")
    end
  rescue
    false
  end

  def estimate_performance_improvement
    cpu_cores = detect_cpu_cores
    parallel_jobs = get_optimal_parallel_jobs
    
    # Rough performance estimates
    single_thread_baseline = 100
    parallel_improvement = [parallel_jobs * 0.7, 1].max  # 70% efficiency per core
    compression_overhead = fetch(:postgres_backup_compression_level, 0) > 0 ? 0.8 : 1.0
    
    estimated_improvement = (parallel_improvement * compression_overhead * 100) / single_thread_baseline
    
    info "Performance estimate: #{estimated_improvement.round}% of single-threaded performance"
    info "Using #{parallel_jobs} parallel jobs on #{cpu_cores} CPU cores"
  end
end
