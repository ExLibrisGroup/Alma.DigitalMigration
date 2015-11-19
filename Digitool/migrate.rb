require 'rest-client'
require 'nokogiri'
require 'aws-sdk'
require 'fileutils'
require 'securerandom'

def log(msg, level='INFO')
	time = Time.new
	time = time.strftime("%Y-%m-%d %H:%M:%S")
	str = "#{time} - #{level.ljust(5,' ')} #{msg}"
	puts str
	if @log_to_file
		@f ||= File.open "#{Time.new.strftime("%Y%m%d%H%M%S")}.log", "a"
		@f.puts str
	end
	true
end

def write_file(bucket, key, content)
	@s3 ||= Aws::S3::Resource.new(credentials: aws_creds(@inst), region: @region || 'us-east-1')
	obj = @s3.bucket(bucket).object(key)
	obj.put(body: content)
end

def aws_creds(profile = 'default')
	# for inside proxy
	Aws.config[:ssl_verify_peer] = false

	# access credentials in credential file - http://tinyurl.com/ljn7r63
	Aws::SharedCredentials.new({ :profile_name => profile })
end

def download_file(url, folder)
	filename = RestClient.head(url).headers[:content_disposition]
	filename = filename[filename.index('filename=')+9..-1] if filename
	# filename may require more fine-tuning
	filename ||= SecureRandom.uuid
	folder = "#{folder}"
	FileUtils.mkdir_p folder
	File.open("#{folder}/#{filename}", "wb") do |file|
		file.write(RestClient.get(url))
	end
	"#{filename}"
end

### Define variables
require_relative 'config'
@log_to_file = true
max_errors = 10
num_of_errors = 0

log "Starting..."
qs = "?verb=ListRecords&set=#{OAI_SET}&metadataPrefix=marc21"
oai_ns = {'oai' => 'http://www.openarchives.org/OAI/2.0/', 'marc' => 'http://www.loc.gov/MARC21/slim'}

ingest_id = SecureRandom.uuid

begin 
	log "Calling OAI with query string #{qs}"
	oai = RestClient.get DIGITOOL_OAI_BASE + qs

	document = Nokogiri::XML(oai)
	recordCount = document.xpath('/oai:OAI-PMH/oai:ListRecords/oai:record', oai_ns).count
	log "#{recordCount} records retrieved"

	# for each record
	document.xpath('/oai:OAI-PMH/oai:ListRecords/oai:record', oai_ns).each do |r| 
		begin
			identifier = r.at_xpath('oai:header/oai:identifier', oai_ns).content
			identifier = identifier[identifier.rindex(':')+1..-1]

			log "Processing record #{identifier}"
			# for each file
			r.xpath('oai:metadata/marc:record/marc:datafield[@tag="856"]/marc:subfield[@code="u"]', oai_ns).each_with_index do |f,i|
				log "Downloading #{f.content} to #{TEMP_FOLDER}/#{identifier}"
				filename=download_file(f.content, "#{TEMP_FOLDER}/#{identifier}")
				local_filename = "#{TEMP_FOLDER}/#{identifier}/#{filename}"
				log "Saved #{local_filename}"

				remote_filename = "#{@inst}/upload/#{IMPORT_PROFILE_ID}/#{ingest_id}/#{identifier}/#{filename}"
				log "Uploading #{remote_filename}"
				File.open("#{local_filename}", 'rb') do |file|
					write_file(S3_BUCKET, 
						"#{remote_filename}", file)
				end

				# Update field
				f.content="#{identifier}/#{filename}"
	            
				# delete file & folder 
				File.delete("#{local_filename}")
				Dir.delete("#{TEMP_FOLDER}/#{identifier}")
			end
		rescue Exception => e  
			num_of_errors += 1
  			log "Failed to process #{identifier}: #{e.message}.", 'ERROR'
  			log "Exiting loop due to too many errors (#{num_of_errors}).", 'ERROR' and 
  				break if num_of_errors > max_errors
  		end
	end

	resumptionToken = 
		document.xpath('/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken', {'oai' => 'http://www.openarchives.org/OAI/2.0/'}).text	
	qs = '?verb=ListRecords&resumptionToken=' + resumptionToken
    
	# Write marcxml file to ingest location
	remote_marc_filename = "#{@inst}/upload/#{IMPORT_PROFILE_ID}/#{ingest_id}/#{resumptionToken}-marc.xml"
    log "Uploading #{remote_marc_filename}"
    marc_document = Nokogiri::XML("<collection></collection")
	marc_document.at_css("collection").
		add_child(document.xpath('/oai:OAI-PMH/oai:ListRecords/oai:record/oai:metadata/marc:record', oai_ns))
    write_file S3_BUCKET, 
    	"#{remote_marc_filename}", 
    	marc_document.to_s
    	
end until resumptionToken == ''

log "Complete"
