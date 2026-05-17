"""DEPRECATED — usar `just run-local` en su lugar.

Este script fue el punto de entrada de desarrollo antes de que existiera
el justfile. Se mantiene para referencia histórica, pero no debe usarse.

  just run-local              # SINK=local, fechas de prueba
  just run-gcs                # SINK=gcs, fechas de prueba
  just run-gcs start=YYYY-MM-DD end=YYYY-MM-DD  # fechas explícitas
"""
