#include <iostream>

#include <arrow/dataset/file_ipc.h>
#include <arrow/table.h>

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>

arrow::Status FillArray(std::shared_ptr<arrow::Array> &arr) {
  arrow::NumericBuilder<arrow::Int64Type> int64_builder;

  auto resize_status = int64_builder.Resize(5);

  if (!resize_status.ok()) {
    return resize_status;
  }

  std::vector<int64_t> int64_values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  auto append_status = int64_builder.AppendValues(int64_values);

  if (!append_status.ok()) {
    return append_status;
  }

  return int64_builder.Finish(&arr);
}

arrow::Status arrowTableScan() {
  // Make a Schema for our Table
  auto schema = arrow::schema({field("a", arrow::int64())});

  // Make a test Array to store in our Table
  std::shared_ptr<arrow::Array> array_a;

  auto fill_array_status = FillArray(array_a);

  if (!fill_array_status.ok()) {
    return fill_array_status;
  }

  // Make the Table
  auto tbl = arrow::Table::Make(schema, {array_a}, 10);

  // Wrap the Table in a Dataset so we can use a Scanner
  std::shared_ptr<arrow::dataset::Dataset> dataset =
      std::make_shared<arrow::dataset::InMemoryDataset>(tbl);

  // Build the Scanner up with a basic filter operation
  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  options->filter = arrow::compute::greater(arrow::compute::field_ref("a"),
                                            arrow::compute::literal(3));

  auto builder = arrow::dataset::ScannerBuilder(dataset, options);
  auto scanner = builder.Finish();

  if (!scanner.ok()) {
    return scanner.status();
  }

  // Perform the Scan and make a Table with the result
  auto result = scanner.ValueUnsafe()->ToTable();

  if (!result.ok()) {
    return result.status();
  }

  // Print it
  std::cout << result.ValueUnsafe()->ToString();

  return arrow::Status::OK();
}

arrow::Status qplTableScan() {
  // Make a Schema for our Table
  auto schema = arrow::schema({field("a", arrow::int64())});

  // Make a test Array to store in our Table
  std::shared_ptr<arrow::Array> array_a;

  auto fill_array_status = FillArray(array_a);

  if (!fill_array_status.ok()) {
    return fill_array_status;
  }

  // Make the Table
  auto tbl = arrow::Table::Make(schema, {array_a}, 10);

  // Wrap the Table in a Dataset so we can use a Scanner
  std::shared_ptr<arrow::dataset::Dataset> dataset =
      std::make_shared<arrow::dataset::InMemoryDataset>(tbl);

  // Build the Scanner up with a basic filter operation
  auto options = std::make_shared<arrow::dataset::ScanOptions>();
  options->filter = arrow::compute::greater(arrow::compute::field_ref("a"),
                                            arrow::compute::literal(3));

  auto builder = arrow::dataset::ScannerBuilder(dataset, options);
  auto scanner = builder.Finish();

  if (!scanner.ok()) {
    return scanner.status();
  }

  // Perform the Scan and make a Table with the result
  auto result = scanner.ValueUnsafe()->ToTable();

  if (!result.ok()) {
    return result.status();
  }

  // Print it
  std::cout << result.ValueUnsafe()->ToString();

  return arrow::Status::OK();
}

int main() {
  arrow::Status st = arrowTableScan();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}