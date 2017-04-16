from processor import REPORTS_BASE_PATH, RABBITMQ_USER, RABBITMQ_HOST, RABBITMQ_PASS, RABBITMQ_PORT, logger, app

if __name__ == "__main__":
    print('''
              m-       ....................`           ...........    .........`             h/
              m-      :mNNNdhyymNNNdyydmNNNm.  -odo.  .yhmNNNNNhyo   .yyNNNNdyy:             h/
              m-     `dMNy:.`  hMMMs  .:odMMy` yMMMs    `-yNMMMy`      -MMmo.`               h/
              m-     +Mm:      hMMMs     `+NM/ :sys.       /mMMMh-   `/NMy.                  h/
              m-     ./`       hMMMs       -shsshdd`        .oMMMm+`-dNd/`                   h/
              m-               hMMMs        `osNMMM`          :dMMMmmNs`                     h/
              m-               hMMMs           hMMM`           .dMMMMs`                      h/
              m-               hMMMs           hMMM`          .yMNmMMMd-                     h/
              m-               hMMMs           hMMM`        `omMy-.sNMMN+`                   h/
              m-               hMMMs           hMMM`       :hMd/    /dMMMd.                  h/
              m-               hMMMs           hMMM`     -hMNs`      `yNMMNs`                h/
              m-            `.:mMMMd-.`     `.:mMMMs..-+yMMMd-.`     ..hMMMMho-.`            d/
              m-            yhdddddddho     :hddddddhhddddddddh:    -hhddddddddh/`.          h/
    ''')

    logger.debug('REPORTS_BASE_PATH: {reports_base_path}, '
                 'RABBITMQ_USER: {rabbitmq_user}, RABBITMQ_PASS: {rabbitmq_pass}, '
                 'RABBITMQ_HOST: {rabbitmq_host}, RABBITMQ_PORT: {rabbitmq_port} '
                 .format(reports_base_path=REPORTS_BASE_PATH,
                         rabbitmq_user=RABBITMQ_USER, rabbitmq_pass=RABBITMQ_PASS,
                         rabbitmq_host=RABBITMQ_HOST, rabbitmq_port=RABBITMQ_PORT))
    # from processor.completo_III import completoIII
    # completoIII()
    app.start()
