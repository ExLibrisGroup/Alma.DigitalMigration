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
	#@f ||= File.open "#{Time.new.strftime("%Y%m%d%H%M%S")}.log", "a"
	#@f.puts str
	true
end

def write_file(bucket, key, path)
	@s3 ||= Aws::S3::Resource.new(credentials: aws_creds(@inst), region: 'us-east-1')
	obj = @s3.bucket(bucket).object(key)
	obj.upload_file(path)
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
	filename ||= SecureRandom.uuid
	folder = "#{folder}"
	FileUtils.mkdir_p folder
	File.open("#{folder}/#{filename}", "wb") do |file|
		file.write(RestClient.get(url))
	end
	"#{filename}"
end

### Define variables
### <<<<<<<<<<<<<<<<
s3_bucket = 'na-st01.ext.exlibrisgroup.com' # bucket name
@inst = 'TR_INTEGRATION_INST' # institution name
import_profile_id = '123456789' # ID of the import profile in ALma
digitool_oai_base = 'http://dc03vg0053eu.hosted.exlibrisgroup.com:8881/OAI-PUB' # URL of OAI in digitool
oai_set = 'foralma' 
temp_folder = '/home/opherk/tmp/'
marc_file = "#{temp_folder}marc.xml"
### <<<<<<<<<<<<<<<<

log "Starting..."
File.write("#{marc_file}", "<collection>", mode: 'w+')
qs = "?verb=ListRecords&set=#{oai_set}&metadataPrefix=marc21"
oai_ns = {'oai' => 'http://www.openarchives.org/OAI/2.0/', 'marc' => 'http://www.loc.gov/MARC21/slim'}

ingest_id = SecureRandom.uuid

begin 
	log "Calling OAI with query string #{qs}"
	oai = RestClient.get digitool_oai_base + qs

	document = Nokogiri::XML(oai)

	recordCount = document.xpath('/oai:OAI-PMH/oai:ListRecords/oai:record', oai_ns).count
	log "#{recordCount} records retrieved"

	# for each record
	document.xpath('/oai:OAI-PMH/oai:ListRecords/oai:record', oai_ns)[0,2].each do |r| 
		identifier = r.at_xpath('oai:header/oai:identifier', oai_ns).content
		identifier = identifier[identifier.rindex(':')+1..-1]

		log "Processing record #{identifier}"
		# for each file
		r.xpath('oai:metadata/marc:record/marc:datafield[@tag="856"]/marc:subfield[@code="u"]', oai_ns).each_with_index do |f,i|
			log "Downloading #{f.content}"
			filename=download_file(f.content, "#{temp_folder}/#{identifier}")
			#filename="A study on the adoption and diffusion of information and communication technologies in the banking industry in Thailand using multiple-criteria decision making and system dynamics approaches..pdf"
			log "Saved #{temp_folder}#{identifier}/#{filename}"

			log "Uploading to S3"
			write_file(s3_bucket, "#{@inst}/upload/#{import_profile_id}/#{ingest_id}/#{identifier}/#{filename}", "#{temp_folder}#{identifier}/#{filename}")

			# Update field
			f.replace("#{identifier}/#{filename}")
            
			# TODO: delete file & folder (if empty)
			#log "Uploaded. Removed file from temp location."

		end
        File.write("#{marc_file}", "#{r.xpath('oai:metadata/marc:record', oai_ns)}", mode: 'a')
	end

    File.write("#{marc_file}", "</collection>", mode: 'a')
    
	resumptionToken = 
		document.xpath('/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken', {'oai' => 'http://www.openarchives.org/OAI/2.0/'}).text	
	qs = '?verb=ListRecords&resumptionToken=' + resumptionToken
    
    File.rename("#{marc_file}", "#{resumptionToken}#{marc_file}")
	# Write marcxml file to ingest location
    log "Uploading #{resumptionToken}#{marc_file} to S3 - uuid=#{ingest_id}"
    write_file(s3_bucket, "#{@inst}/upload/#{import_profile_id}/#{ingest_id}/marc.xml", "#{marc_file}")
    
end until resumptionToken == ''

log "Complete"
