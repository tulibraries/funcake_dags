# frozen_string_literal: true

def collect_push_totals(output_lines)
  output_lines
    .select { |l| l.match(/process: \d+ records/) }
    .map { |l| l.scan(/process: (\d+) records/) }.flatten
    .sum(&:to_i)
end

def published_total(output_lines)
  solr_count = ENV["SOLR_PUBLISHED_COUNT"]
  return solr_count if solr_count&.match?(/\A\d+\z/)

  collect_push_totals(output_lines)
end

puts "{ 'published': '#{published_total($stdin)}' }"
