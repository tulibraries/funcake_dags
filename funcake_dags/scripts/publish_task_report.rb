# frozen_string_literal: true

def collect_push_totals(output_lines)
  output_lines
    .select { |l| l.match(/process: \d+ records/) }
    .map { |l| l.scan(/process: (\d+) records/) }.flatten
    .map(&:to_i).reduce(&:+)
end

puts "{ 'published': '#{collect_push_totals($stdin)}' }"
