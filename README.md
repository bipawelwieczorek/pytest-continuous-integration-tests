# Continuous Integration Tests

## Sample test executions

Jupyter notebook

```python
HTML_REPORT = 'report_file.html'
CONNECTION_NAME = 'toml connection name'

!pytest sa\mple_test --self-contained-html --html="{HTML_REPORT}" --metadata connection_name {CONNECTION_NAME}
```
