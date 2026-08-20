# frozen_string_literal: true

def collect_push_totals(output_lines)
  output_lines
    .select { |l| l.match(/process: \d+ records/) }
    .map { |l| l.scan(/process: (\d+) records/) }.flatten
    .sum(&:to_i)
end

puts "{ 'published': '#{collect_push_totals($stdin)}' }"
