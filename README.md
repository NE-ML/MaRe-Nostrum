# MaRe-Nostrum
Это наша реализация многопоточного MapReduce на C++ (aka HW2 on MP C++ course at VK x BMSTU)    

## setInputFiles
Изменяет входные файлы, которые будут использоваться в дальнейшем при вызове метода runMapReduceFramework.

## setMaxSimultaneousWorkers
Изменяет максимальное количество одновременно выполняемых потоков.

## setNumReducers
Изменяет количество редьюсеров.

## setOutputDir
Изменяет директорию, в которую будут записаны результаты работы.

## setTmpDir
Изменяет директорию, в которой будет происходить работа.

## setMapper
Изменяет функцию, которая будет использоваться в качестве маппера.

## setReducer
Изменяет функцию, которая будет использоваться в качестве редьюсера.

## Сборка проекта
```bash
mkdir -p ./build/
cmake -S . -B ./build/
cmake --build ./build/
```

## Запуск тестов
```bash
./build/test/unit/unit_tests
```

