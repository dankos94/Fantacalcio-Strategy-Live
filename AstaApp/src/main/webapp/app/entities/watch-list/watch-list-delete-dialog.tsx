import React, { useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Button, Modal, ModalBody, ModalFooter, ModalHeader } from 'reactstrap';

import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';
import { deleteEntity, getEntity } from './watch-list.reducer';

export const WatchListDeleteDialog = () => {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const { id } = useParams<'id'>();

  const [loadModal, setLoadModal] = useState(false);

  useEffect(() => {
    dispatch(getEntity(id));
    setLoadModal(true);
  }, []);

  const watchListEntity = useAppSelector(state => state.watchList.entity);
  const updateSuccess = useAppSelector(state => state.watchList.updateSuccess);

  const handleClose = () => {
    navigate('/watch-list');
  };

  useEffect(() => {
    if (updateSuccess && loadModal) {
      handleClose();
      setLoadModal(false);
    }
  }, [updateSuccess]);

  const confirmDelete = () => {
    dispatch(deleteEntity(watchListEntity.id));
  };

  return (
    <Modal isOpen toggle={handleClose}>
      <ModalHeader toggle={handleClose} data-cy="watchListDeleteDialogHeading">
        Conferma operazione di cancellazione
      </ModalHeader>
      <ModalBody id="astaAppApp.watchList.delete.question">Sei sicuro di volere eliminare Watch List {watchListEntity.id}?</ModalBody>
      <ModalFooter>
        <Button color="secondary" onClick={handleClose}>
          <FontAwesomeIcon icon="ban" />
          &nbsp; Annulla
        </Button>
        <Button id="jhi-confirm-delete-watchList" data-cy="entityConfirmDeleteButton" color="danger" onClick={confirmDelete}>
          <FontAwesomeIcon icon="trash" />
          &nbsp; Elimina
        </Button>
      </ModalFooter>
    </Modal>
  );
};

export default WatchListDeleteDialog;
