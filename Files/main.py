import sys
import pandas as pd
from PyQt6 import QtWidgets, QtCore, QtGui
from PyQt6.QtCore import Qt, QAbstractTableModel
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure

# Importar interfaz generada
from gui.main_window import Ui_MainWindow

# ----------------------------------------------------------
# Módulo: Modelo de Datos para Pandas
# ----------------------------------------------------------
class PandasModel(QAbstractTableModel):
    def __init__(self, data):
        super().__init__()
        self._data = data

    def data(self, index, role):
        if role == Qt.ItemDataRole.DisplayRole:
            value = self._data.iloc[index.row(), index.column()]
            return str(value)

    def rowCount(self, index):
        return self._data.shape[0]

    def columnCount(self, index):
        return self._data.shape[1]

    def headerData(self, section, orientation, role):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return str(self._data.columns[section])
            else:
                return str(self._data.index[section])
        return None

# ----------------------------------------------------------
# Módulo: Gráficos Matplotlib
# ----------------------------------------------------------
class AnalysisCanvas(FigureCanvasQTAgg):
    def __init__(self, parent=None):
        self.fig = Figure(figsize=(8, 4), tight_layout=True)
        super().__init__(self.fig)
        self.ax = self.fig.add_subplot(111)
        
    def plot_series(self, data, title="Evolución Temporal"):
        self.ax.clear()
        data.plot(ax=self.ax)
        self.ax.set_title(title)
        self.draw()

# ----------------------------------------------------------
# Módulo: Hilo de Entrenamiento
# ----------------------------------------------------------
class TrainingThread(QtCore.QThread):
    progress_updated = QtCore.pyqtSignal(int)
    training_finished = QtCore.pyqtSignal(object)
    error_occurred = QtCore.pyqtSignal(str)

    def __init__(self, data, model_type, params):
        super().__init__()
        self.data = data
        self.model_type = model_type
        self.params = params

    def run(self):
        try:
            # Simulación de entrenamiento
            for i in range(101):
                self.progress_updated.emit(i)
                QtCore.QThread.msleep(50)
            
            # Resultado simulado
            result = {
                'model_type': self.model_type,
                'accuracy': 0.85,
                'features': list(self.data.columns)
            }
            self.training_finished.emit(result)
            
        except Exception as e:
            self.error_occurred.emit(str(e))

# ----------------------------------------------------------
# Clase Principal de la Aplicación
# ----------------------------------------------------------
class BAMAnalyzerApp(QtWidgets.QMainWindow, Ui_MainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi(self)

        # Configurar el widgetGraph programáticamente
        self.widgetGraph.setLayout(QtWidgets.QVBoxLayout())  # <-- Añade esto
        self.canvas = AnalysisCanvas()
        self.widgetGraph.layout().addWidget(self.canvas)  # <-- Usa .layout()
        # Variables de estado
        self.current_data = None
        self.current_model = None
        
        # Configuración inicial
        self._setup_ui()
        self._connect_signals()
        
    def _setup_ui(self):
        """Inicializa componentes de la interfaz"""
        # Configurar gráficos
        self.canvas = AnalysisCanvas()
        self.graphLayout.addWidget(self.canvas)
        
        # Configurar modelo de tabla
        self.tableView.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        
        # Cargar estilos
        self.load_styles("Files/gui/styles.qss")
        
    def _connect_signals(self):
        """Conectar señales y slots"""
        self.actionLoadData.triggered.connect(self.load_data)
        self.btnTrain.clicked.connect(self.start_training)
        self.cmbModelType.currentTextChanged.connect(self.update_model_params)

    def load_styles(self, filename):
        """Carga estilos desde archivo QSS"""
        with open(filename, "r") as f:
            self.setStyleSheet(f.read())

    def load_data(self):
        """Carga los datos BAM procesados para una empresa seleccionada"""
        from Bam_pipeline import ejecutar_pipeline_empresa  # Asume que tu código está modularizado

        empresa, ok = QtWidgets.QInputDialog.getText(
            self, "Nombre de la empresa", "Introduce el nombre exacto de la empresa:"
        )

        if ok and empresa:
            try:
                resultado = ejecutar_pipeline_empresa(empresa)
                if resultado.empty:
                    QtWidgets.QMessageBox.information(self, "Sin datos", f"No se encontraron datos para '{empresa}'")
                    return

                self.current_data = resultado
                self.update_data_display()
                self.statusbar.showMessage(f"Datos cargados para empresa: {empresa}", 5000)
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error", f"Error al cargar datos:\n{str(e)}")

    def update_data_display(self):
        """Actualiza la visualización de datos"""
        # Tabla de datos
        model = PandasModel(self.current_data)
        self.tableView.setModel(model)
        
        # Gráfico temporal
        self.canvas.plot_series(self.current_data.iloc[:, 0])  # Primera columna
        
        # Habilitar controles
        self.btnTrain.setEnabled(True)
        self.cmbModelType.setEnabled(True)

    def update_model_params(self, model_type):
        """Actualiza parámetros según el modelo seleccionado"""
        if "Red Neuronal" in model_type:
            self.stackedParams.setCurrentIndex(0)
        elif "Random Forest" in model_type:
            self.stackedParams.setCurrentIndex(1)
        else:
            self.stackedParams.setCurrentIndex(2)

    def start_training(self):
        """Inicia el proceso de entrenamiento"""
        if self.current_data is None:
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Primero cargue datos BAM")
            return
            
        model_type = self.cmbModelType.currentText()
        params = {
            'layers': self.spinLayers.value(),
            'rate': self.dspinLearningRate.value()
        }
        
        # Configurar y ejecutar hilo
        self.thread = TrainingThread(self.current_data, model_type, params)
        self.thread.progress_updated.connect(self.progressBar.setValue)
        self.thread.training_finished.connect(self.on_training_complete)
        self.thread.error_occurred.connect(self.show_error)
        self.thread.start()
        
        # Bloquear controles durante el entrenamiento
        self.set_controls_enabled(False)

    def on_training_complete(self, result):
        """Maneja los resultados del entrenamiento"""
        self.set_controls_enabled(True)
        self.statusbar.showMessage(f"Entrenamiento completado: {result['model_type']} - Precisión: {result['accuracy']:.2%}")
        
        # Mostrar resultados
        self.txtResults.appendPlainText(
            f"\n=== Modelo Entrenado ===\n"
            f"Tipo: {result['model_type']}\n"
            f"Características usadas: {', '.join(result['features'])}\n"
            f"Precisión: {result['accuracy']:.2%}"
        )

    def show_error(self, message):
        """Muestra errores en un cuadro de diálogo"""
        QtWidgets.QMessageBox.critical(self, "Error", message)
        self.set_controls_enabled(True)

    def set_controls_enabled(self, enabled):
        """Habilita/deshabilita controles durante el entrenamiento"""
        self.btnTrain.setEnabled(enabled)
        self.cmbModelType.setEnabled(enabled)
        self.actionLoadData.setEnabled(enabled)
        self.progressBar.setValue(0)

# ----------------------------------------------------------
# Ejecución de la Aplicación
# ----------------------------------------------------------
if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = BAMAnalyzerApp()
    window.show()
    sys.exit(app.exec())