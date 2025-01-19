# Configuration notes

## Windows
Powershell doesn't support UTF-8 encoding out the box. You need to run:

```
$OutputEncoding = [console]::InputEncoding = [console]::OutputEncoding = New-Object System.Text.UTF8Encoding
```

To show terminal plot output.
