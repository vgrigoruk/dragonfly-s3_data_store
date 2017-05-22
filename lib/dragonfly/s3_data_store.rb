require 'aws-sdk'
require 'dragonfly'
require 'cgi'
require 'securerandom'

Dragonfly::App.register_datastore(:s3) { Dragonfly::S3DataStore }

module Dragonfly
  class S3DataStore

    # # Exceptions
    # class NotConfigured < RuntimeError
    # end

    SUBDOMAIN_PATTERN = /^[a-z0-9][a-z0-9.-]+[a-z0-9]$/

    def initialize(opts={})
      @bucket_name = opts[:bucket_name]
      @access_key_id = opts[:access_key_id]
      @secret_access_key = opts[:secret_access_key]
      @region = opts[:region]
      @acl = opts[:acl] || 'private'
      @storage_class = opts[:storage_class] || 'STANDARD'
      @url_scheme = opts[:url_scheme] || 'http'
      @url_host = opts[:url_host]
      @root_path = opts[:root_path]
    end

    attr_accessor :bucket_name, :access_key_id, :secret_access_key, :region, :url_scheme, :url_host, :root_path

    def write(content, opts={})
      ensure_bucket_initialized
      content_type = opts.delete(:content_type) { |_| content.mime_type }
      uid = opts[:path] || generate_uid(content.name || 'file')
      content.file do |f|
        s3_client.put_object(opts.merge(bucket: bucket_name,
                                        key: full_path(uid),
                                        content_type: content_type,
                                        metadata: content.meta,
                                        body: f,
                                        acl: @acl,
                                        storage_class: @storage_class))
      end
      uid
    end

    def read(uid)
      response = s3_client.get_object(bucket: bucket_name, key: full_path(uid))
      [response.body.read, response.metadata]
    rescue Aws::S3::Errors::NoSuchKey => _
      nil
    end

    def destroy(uid)
      s3_client.delete_object(bucket: bucket_name, key: full_path(uid))
    rescue => e
      Dragonfly.warn("#{self.class.name} destroy error: #{e}")
    end

    def url_for(uid, opts={})
      s3 = Aws::S3::Resource.new.bucket(bucket_name)
      if expires = opts[:expires]
        s3.get_object_https_url(bucket_name, full_path(uid), expires, {:query => opts[:query]})
      else
        scheme = opts[:scheme] || url_scheme
        host = opts[:host] || url_host || (
        bucket_name =~ SUBDOMAIN_PATTERN ? "#{bucket_name}.s3.amazonaws.com" : "s3.amazonaws.com/#{bucket_name}"
        )
        "#{scheme}://#{host}/#{full_path(uid)}"
      end
    end

    def bucket_exists?
      s3_client.get_bucket_location(bucket: @bucket_name)
      true
    rescue => _
      false
    end

    private

    def s3_client
      @s3_client ||= begin
        opts = {}
        opts[:region] = @region if @region
        opts[:access_key_id] = @access_key_id if @access_key_id
        opts[:secret_access_key] = @secret_access_key if @secret_access_key
        Aws::S3::Client.new(opts)
      end
    end

    def bucket
      @bucket ||= Aws::S3::Resource.new.bucket(bucket_name)
    end

    def ensure_bucket_initialized
      unless @bucket_initialized
        s3_client.create_bucket(bucket: @bucket_name, create_bucket_configuration: {location_constraint: region}) unless bucket_exists?
        @bucket_initialized = true
      end
    end

    def generate_uid(name)
      "#{Time.now.strftime '%Y/%m/%d/%H/%M/%S'}/#{SecureRandom.uuid}/#{name}"
    end

    def full_path(uid)
      File.join *[root_path, uid].compact
    end
  end
end
